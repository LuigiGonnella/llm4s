package org.llm4s.knowledgegraph.storage

import org.llm4s.knowledgegraph.{ Edge, Graph, Node }
import org.llm4s.types.Result
import org.llm4s.error.ProcessingError
import org.llm4s.types.TryOps
import com.zaxxer.hikari.HikariDataSource
import java.sql.{ Connection, ResultSet }
import java.nio.file.Path
import scala.util.{ Try, Using }

/**
 * SQLite-based implementation of GraphStore with connection pooling.
 *
 * Thread-safety: This implementation is thread-safe via SQLite transactions and HikariCP connection pooling.
 * Each operation uses its own connection from the pool with automatic transaction management.
 *
 * Persistence: Graphs are persisted to disk via SQLite database file.
 * Suitable for production use with moderate graph sizes (<10M nodes/edges).
 *
 * Schema:
 *   nodes: id (PK), label, properties (JSON)
 *   edges: id (PK), source_id (FK), target_id (FK), relationship, properties (JSON), UNIQUE(source_id, target_id, relationship)
 *
 * @param dbPath Path to SQLite database file
 * @param poolSize Connection pool size (default 10)
 */
class SQLiteGraphStore(dbPath: Path, poolSize: Int = 10) extends GraphStore {
  private val dataSource = createDataSource(dbPath, poolSize)

  // Initialize schema on creation
  initializeSchema()

  /**
   * Creates and configures HikariCP connection pool
   */
  private def createDataSource(path: Path, size: Int): HikariDataSource = {
    val ds = new HikariDataSource()
    ds.setJdbcUrl(s"jdbc:sqlite:${path.toAbsolutePath}")
    ds.setMaximumPoolSize(size)
    ds.setMinimumIdle(1)
    ds.setConnectionTimeout(5000)
    ds.setIdleTimeout(600000) // 10 minutes
    // Enable foreign keys for all connections from the pool
    ds.setConnectionInitSql("PRAGMA foreign_keys = ON")
    ds
  }

  /**
   * Initialize database schema if not already present
   */
  private def initializeSchema(): Unit =
    withConnection { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        // Nodes table
        stmt.execute("""
          CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            properties TEXT NOT NULL DEFAULT '{}'
          )
        """)

        // Edges table with composite unique constraint
        stmt.execute("""
          CREATE TABLE IF NOT EXISTS edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            relationship TEXT NOT NULL,
            properties TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
            FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE,
            UNIQUE(source_id, target_id, relationship)
          )
        """)

        // Create indexes for performance
        stmt.execute("CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id)")
        stmt.execute("CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id)")
        stmt.execute("CREATE INDEX IF NOT EXISTS idx_edges_relationship ON edges(relationship)")
        stmt.execute("CREATE INDEX IF NOT EXISTS idx_nodes_label ON nodes(label)")
        Right(())
      }
    }

  /**
   * Validates that a property key contains only safe characters to prevent SQL injection.
   * Property keys must contain only alphanumeric characters, underscores, and hyphens.
   */
  private def validatePropertyKey(key: String): Result[Unit] =
    if (key.matches("^[a-zA-Z0-9_-]+$")) Right(())
    else
      Left(
        ProcessingError(
          "invalid_property_key",
          s"Property key '$key' contains unsafe characters. Only alphanumeric, underscore, and hyphen are allowed."
        )
      )

  override def upsertNode(node: Node): Result[Unit] =
    withConnection { conn =>
      val sql = """
        INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          label = excluded.label,
          properties = excluded.properties
      """

      Using.resource(conn.prepareStatement(sql)) { stmt =>
        stmt.setString(1, node.id)
        stmt.setString(2, node.label)
        stmt.setString(3, ujson.write(ujson.Obj.from(node.properties)))
        stmt.executeUpdate()
        Right(())
      }
    }

  override def upsertEdge(edge: Edge): Result[Unit] =
    withConnection { conn =>
      // Verify both nodes exist
      val verifySQL = "SELECT id FROM nodes WHERE id = ? OR id = ?"
      val existing = Using.resource(conn.prepareStatement(verifySQL)) { verifyStmt =>
        verifyStmt.setString(1, edge.source)
        verifyStmt.setString(2, edge.target)
        Using.resource(verifyStmt.executeQuery()) { rs =>
          val ids = scala.collection.mutable.Set[String]()
          while (rs.next())
            ids += rs.getString("id")
          ids
        }
      }

      if (!existing.contains(edge.source) || !existing.contains(edge.target)) {
        Left(
          ProcessingError(
            "edge_validation",
            s"Edge(${edge.source}->${edge.target}): source or target node does not exist"
          )
        )
      } else {
        val sql = """
          INSERT INTO edges (source_id, target_id, relationship, properties) VALUES (?, ?, ?, ?)
          ON CONFLICT(source_id, target_id, relationship) DO UPDATE SET
            properties = excluded.properties
        """

        Using.resource(conn.prepareStatement(sql)) { stmt =>
          stmt.setString(1, edge.source)
          stmt.setString(2, edge.target)
          stmt.setString(3, edge.relationship)
          stmt.setString(4, ujson.write(ujson.Obj.from(edge.properties)))
          stmt.executeUpdate()
          Right(())
        }
      }
    }

  override def getNode(id: String): Result[Option[Node]] =
    withConnection { conn =>
      val sql = "SELECT id, label, properties FROM nodes WHERE id = ?"
      Using.resource(conn.prepareStatement(sql)) { stmt =>
        stmt.setString(1, id)
        Using.resource(stmt.executeQuery()) { rs =>
          if (rs.next()) {
            parseNode(rs).map(Some(_))
          } else {
            Right(None)
          }
        }
      }
    }

  override def getNeighbors(nodeId: String, direction: Direction = Direction.Both): Result[Seq[EdgeNodePair]] =
    withConnection { conn =>
      // First check if node exists
      val nodeExists = Using.resource(conn.prepareStatement("SELECT id FROM nodes WHERE id = ?")) { checkStmt =>
        checkStmt.setString(1, nodeId)
        Using.resource(checkStmt.executeQuery())(rs => rs.next())
      }

      if (!nodeExists) {
        Right(Seq.empty)
      } else {
        val sql = direction match {
          case Direction.Outgoing =>
            """
            SELECT e.source_id, e.target_id, e.relationship, e.properties,
                   n.id, n.label, n.properties as node_properties
            FROM edges e
            JOIN nodes n ON e.target_id = n.id
            WHERE e.source_id = ?
          """
          case Direction.Incoming =>
            """
            SELECT e.source_id, e.target_id, e.relationship, e.properties,
                   n.id, n.label, n.properties as node_properties
            FROM edges e
            JOIN nodes n ON e.source_id = n.id
            WHERE e.target_id = ?
          """
          case Direction.Both =>
            """
            SELECT e.source_id, e.target_id, e.relationship, e.properties,
                   n.id, n.label, n.properties as node_properties
            FROM edges e
            JOIN nodes n ON (CASE WHEN e.source_id = ? THEN e.target_id = n.id ELSE e.source_id = n.id END)
            WHERE e.source_id = ? OR e.target_id = ?
          """
        }

        Using.resource(conn.prepareStatement(sql)) { stmt =>
          if (direction == Direction.Both) {
            stmt.setString(1, nodeId)
            stmt.setString(2, nodeId)
            stmt.setString(3, nodeId)
          } else {
            stmt.setString(1, nodeId)
          }

          Using.resource(stmt.executeQuery()) { rs =>
            val pairs                                        = scala.collection.mutable.ArrayBuffer[EdgeNodePair]()
            var parseError: Option[org.llm4s.error.LLMError] = None
            while (rs.next())
              if (parseError.isEmpty) {
                val parsedPair = for {
                  edgeProps <- parseProperties(rs.getString("properties"))
                  nodeProps <- parseProperties(rs.getString("node_properties"))
                } yield {
                  val edge = Edge(
                    rs.getString("source_id"),
                    rs.getString("target_id"),
                    rs.getString("relationship"),
                    edgeProps
                  )
                  val node = Node(
                    rs.getString("id"),
                    rs.getString("label"),
                    nodeProps
                  )
                  EdgeNodePair(edge, node)
                }

                parsedPair match {
                  case Left(error) => parseError = Some(error)
                  case Right(pair) => pairs += pair
                }
              }
            parseError match {
              case Some(error) => Left(error)
              case None        => Right(pairs.toSeq)
            }
          }
        }
      }
    }

  override def query(filter: GraphFilter): Result[Graph] = {
    // Validate property key before using it in SQL to prevent injection
    val validationResult = filter.propertyKey match {
      case Some(key) => validatePropertyKey(key)
      case None      => Right(())
    }

    validationResult.flatMap { _ =>
      withConnection { conn =>
        // Build query with optional filters
        var nodeSql    = "SELECT id, label, properties FROM nodes WHERE 1=1"
        val nodeParams = scala.collection.mutable.ListBuffer[String]()

        if (filter.nodeLabel.isDefined) {
          nodeSql += " AND label = ?"
          nodeParams += filter.nodeLabel.get
        }

        if (filter.propertyKey.isDefined && filter.propertyValue.isDefined) {
          // Cast json_extract result to text for numeric/string comparison
          // Property key is validated above, safe to concatenate
          nodeSql += " AND CAST(json_extract(properties, '$." + filter.propertyKey.get + "') AS TEXT) = ?"
          nodeParams += filter.propertyValue.get
        }

        // Get matching nodes
        val nodes = scala.collection.mutable.Map[String, Node]()
        val nodeLoadResult = Using.resource(conn.prepareStatement(nodeSql)) { stmt =>
          nodeParams.zipWithIndex.foreach { case (param, idx) =>
            stmt.setString(idx + 1, param)
          }
          Using.resource(stmt.executeQuery()) { rs =>
            var parseError: Option[org.llm4s.error.LLMError] = None
            while (rs.next())
              if (parseError.isEmpty) {
                parseNode(rs) match {
                  case Left(error) => parseError = Some(error)
                  case Right(node) => nodes += (node.id -> node)
                }
              }
            parseError match {
              case Some(error) => Left(error)
              case None        => Right(())
            }
          }
        }

        // Get edges between matching nodes
        // Property key is validated above, safe to concatenate in both subqueries
        var edgeSql = """
          SELECT source_id, target_id, relationship, properties FROM edges
          WHERE source_id IN (SELECT id FROM nodes WHERE 1=1
        """
        edgeSql += (if (filter.nodeLabel.isDefined) " AND label = ?" else "")
        edgeSql += (if (filter.propertyKey.isDefined && filter.propertyValue.isDefined)
                      " AND CAST(json_extract(properties, '$." + filter.propertyKey.get + "') AS TEXT) = ?"
                    else "")
        edgeSql += ") AND target_id IN (SELECT id FROM nodes WHERE 1=1"
        edgeSql += (if (filter.nodeLabel.isDefined) " AND label = ?" else "")
        edgeSql += (if (filter.propertyKey.isDefined && filter.propertyValue.isDefined)
                      " AND CAST(json_extract(properties, '$." + filter.propertyKey.get + "') AS TEXT) = ?"
                    else "")
        edgeSql += ")"

        if (filter.relationshipType.isDefined) {
          edgeSql += " AND relationship = ?"
        }

        val edgeParams = scala.collection.mutable.ListBuffer[String]()
        // Add node filter params twice (once for source, once for target)
        nodeParams.foreach { p =>
          edgeParams += p
          edgeParams += p
        }
        if (filter.relationshipType.isDefined) {
          edgeParams += filter.relationshipType.get
        }

        val edges = scala.collection.mutable.ArrayBuffer[Edge]()
        val edgeLoadResult = Using.resource(conn.prepareStatement(edgeSql)) { stmt =>
          edgeParams.zipWithIndex.foreach { case (param, idx) =>
            stmt.setString(idx + 1, param)
          }
          Using.resource(stmt.executeQuery()) { rs =>
            var parseError: Option[org.llm4s.error.LLMError] = None
            while (rs.next())
              if (parseError.isEmpty) {
                parseProperties(rs.getString("properties")) match {
                  case Left(error) => parseError = Some(error)
                  case Right(props) =>
                    edges += Edge(
                      rs.getString("source_id"),
                      rs.getString("target_id"),
                      rs.getString("relationship"),
                      props
                    )
                }
              }
            parseError match {
              case Some(error) => Left(error)
              case None        => Right(())
            }
          }
        }

        for {
          _ <- nodeLoadResult
          _ <- edgeLoadResult
        } yield Graph(nodes.toMap, edges.toList)
      }
    }
  }

  override def traverse(startId: String, config: TraversalConfig = TraversalConfig()): Result[Seq[Node]] =
    withConnection { conn =>
      GraphTraversal.bfs(startId, config)(
        nodeId =>
          Using.resource(conn.prepareStatement("SELECT id, label, properties FROM nodes WHERE id = ?")) { stmt =>
            stmt.setString(1, nodeId)
            Using.resource(stmt.executeQuery())(rs => if (rs.next()) parseNode(rs).map(Some(_)) else Right(None))
          },
        (currentNodeId, direction) => {
          val sql = direction match {
            case Direction.Outgoing =>
              "SELECT target_id FROM edges WHERE source_id = ?"
            case Direction.Incoming =>
              "SELECT source_id FROM edges WHERE target_id = ?"
            case Direction.Both =>
              "SELECT target_id FROM edges WHERE source_id = ? UNION SELECT source_id FROM edges WHERE target_id = ?"
          }

          Using.resource(conn.prepareStatement(sql)) { stmt =>
            stmt.setString(1, currentNodeId)
            if (direction == Direction.Both) {
              stmt.setString(2, currentNodeId)
            }
            Using.resource(stmt.executeQuery()) { rs =>
              val ids = scala.collection.mutable.ArrayBuffer[String]()
              while (rs.next())
                ids += rs.getString(1)
              Right(ids.toSeq)
            }
          }
        }
      )
    }

  override def deleteNode(id: String): Result[Unit] =
    withConnection { conn =>
      val sql = "DELETE FROM nodes WHERE id = ?"
      Using.resource(conn.prepareStatement(sql)) { stmt =>
        stmt.setString(1, id)
        stmt.executeUpdate()
        Right(())
      }
      // Edges are automatically deleted via CASCADE FK
    }

  override def deleteEdge(source: String, target: String, relationship: String): Result[Unit] =
    withConnection { conn =>
      val sql = "DELETE FROM edges WHERE source_id = ? AND target_id = ? AND relationship = ?"
      Using.resource(conn.prepareStatement(sql)) { stmt =>
        stmt.setString(1, source)
        stmt.setString(2, target)
        stmt.setString(3, relationship)
        stmt.executeUpdate()
        Right(())
      }
    }

  override def loadAll(): Result[Graph] =
    withConnection { conn =>
      val nodes = scala.collection.mutable.Map[String, Node]()
      val nodeLoadResult = Using.resource(conn.createStatement()) { nodeStmt =>
        Using.resource(nodeStmt.executeQuery("SELECT id, label, properties FROM nodes")) { rs =>
          var parseError: Option[org.llm4s.error.LLMError] = None
          while (rs.next())
            if (parseError.isEmpty) {
              parseNode(rs) match {
                case Left(error) => parseError = Some(error)
                case Right(node) => nodes += (node.id -> node)
              }
            }
          parseError match {
            case Some(error) => Left(error)
            case None        => Right(())
          }
        }
      }

      val edges = scala.collection.mutable.ArrayBuffer[Edge]()
      val edgeLoadResult = Using.resource(conn.createStatement()) { edgeStmt =>
        Using.resource(
          edgeStmt
            .executeQuery(
              "SELECT source_id, target_id, relationship, properties FROM edges"
            )
        ) { rs =>
          var parseError: Option[org.llm4s.error.LLMError] = None
          while (rs.next())
            if (parseError.isEmpty) {
              parseProperties(rs.getString("properties")) match {
                case Left(error) => parseError = Some(error)
                case Right(props) =>
                  edges += Edge(
                    rs.getString("source_id"),
                    rs.getString("target_id"),
                    rs.getString("relationship"),
                    props
                  )
              }
            }
          parseError match {
            case Some(error) => Left(error)
            case None        => Right(())
          }
        }
      }

      for {
        _ <- nodeLoadResult
        _ <- edgeLoadResult
      } yield Graph(nodes.toMap, edges.toList)
    }

  override def stats(): Result[GraphStats] =
    withConnection { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        val nodeCount = Using.resource(stmt.executeQuery("SELECT COUNT(*) as count FROM nodes")) { rs =>
          rs.next()
          rs.getLong("count")
        }

        val edgeCount = Using.resource(stmt.executeQuery("SELECT COUNT(*) as count FROM edges")) { rs =>
          rs.next()
          rs.getLong("count")
        }

        val avgDegree = if (nodeCount > 0) (edgeCount * 2.0) / nodeCount else 0.0

        val densestNodeId = Using.resource(
          stmt
            .executeQuery("""
        SELECT node_id, SUM(degree) as total_degree FROM (
          SELECT source_id as node_id, COUNT(*) as degree FROM edges
          GROUP BY source_id
          UNION ALL
          SELECT target_id as node_id, COUNT(*) as degree FROM edges
          GROUP BY target_id
        ) GROUP BY node_id
        ORDER BY total_degree DESC
        LIMIT 1
      """)
        )(rs => if (rs.next()) Some(rs.getString("node_id")) else None)

        Right(GraphStats(nodeCount, edgeCount, avgDegree, densestNodeId))
      }
    }

  /**
   * Parse Node from ResultSet
   */
  private def parseNode(rs: ResultSet): Result[Node] =
    parseProperties(rs.getString("properties")).map { props =>
      Node(
        id = rs.getString("id"),
        label = rs.getString("label"),
        properties = props
      )
    }

  /**
   * Parse properties from JSON string
   */
  private def parseProperties(json: String): Result[Map[String, ujson.Value]] =
    Try(ujson.read(json).obj.toMap).toResult.left.map { error =>
      ProcessingError("sqlite_parse_properties", s"Failed to parse properties JSON: ${error.message}")
    }

  /**
   * Execute operation with connection from pool
   */
  private def withConnection[T](fn: Connection => Result[T]): Result[T] =
    Try(Using.resource(dataSource.getConnection)(fn)).toResult.left.map { error =>
      ProcessingError("sqlite_operation", s"Database operation failed: ${error.message}")
    }.flatten

  /**
   * Closes the connection pool and releases resources.
   * Call this when the GraphStore is no longer needed.
   */
  def close(): Unit =
    if (!dataSource.isClosed) {
      dataSource.close()
    }
}
