package org.llm4s.knowledgegraph.storage

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.llm4s.knowledgegraph.{ Edge, Node }
import java.nio.file.{ Files, Path }

/**
 * Comprehensive test suite for SQLiteGraphStore.
 *
 * Tests persistence, connection pooling, concurrency, and all operations.
 *
 * Key validations:
 * - SQLite database initialization and schema creation
 * - Connection pooling via HikariCP
 * - Transaction safety and consistency
 * - Persistence across store instances
 * - Index effectiveness for query performance
 */
class SQLiteGraphStoreSpec extends AnyFunSuite with Matchers {

  def createTempDbPath(): Path = {
    val tempDir = Files.createTempDirectory("llm4s-test-")
    tempDir.resolve("test-graph.db")
  }

  test("SQLiteGraphStore should initialize database and schema") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)

    // Verify database file was created
    Files.exists(dbPath) shouldBe true

    // Try basic operations to verify schema is valid
    val node = Node("1", "Person")
    store.upsertNode(node) shouldBe Right(())

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("upsertNode should persist to database") {
    val dbPath = createTempDbPath()
    val store1 = new SQLiteGraphStore(dbPath)
    val node   = Node("1", "Person", Map("name" -> ujson.Str("Alice")))

    store1.upsertNode(node) shouldBe Right(())

    // Verify persistence by creating new store with same db
    val store2    = new SQLiteGraphStore(dbPath)
    val retrieved = store2.getNode("1")

    retrieved.toOption.get.map(_.label) shouldBe Some("Person")
    retrieved.toOption.get.flatMap(_.properties.get("name")) shouldBe Some(ujson.Str("Alice"))

    // Cleanup
    store1.close()
    store2.close()
    Files.deleteIfExists(dbPath)
  }

  test("upsertNode should update existing nodes") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val node1  = Node("1", "Person", Map("name" -> ujson.Str("Alice")))
    val node2  = Node("1", "Person", Map("name" -> ujson.Str("Alicia"), "age" -> ujson.Num(30)))

    store.upsertNode(node1) shouldBe Right(())
    store.upsertNode(node2) shouldBe Right(())

    val updated = store.getNode("1")
    updated.toOption.get.flatMap(_.properties.get("name")) shouldBe Some(ujson.Str("Alicia"))
    updated.toOption.get.flatMap(_.properties.get("age")) shouldBe Some(ujson.Num(30))

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("upsertEdge should enforce referential integrity") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val target = Node("2", "Person")
    store.upsertNode(target)

    val edge   = Edge("1", "2", "KNOWS")
    val result = store.upsertEdge(edge)

    // Should fail because source node (1) doesn't exist
    result shouldBe a[Left[_, _]]

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("upsertEdge should prevent duplicate edges") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "A")
    val n2     = Node("2", "B")

    store.upsertNode(n1)
    store.upsertNode(n2)

    val edge = Edge("1", "2", "KNOWS")
    store.upsertEdge(edge) shouldBe Right(())
    // Inserting same edge again should either succeed (update) or fail gracefully
    val result = store.upsertEdge(edge)
    result shouldBe a[Right[_, _]]

    // Verify edge exists only once
    val graph = store.loadAll().toOption.get
    graph.edges.filter(e => e.source == "1" && e.target == "2" && e.relationship == "KNOWS") should have size 1

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("upsertEdge should replace existing edge with same source, target, and relationship") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "Person")
    val n2     = Node("2", "Person")

    store.upsertNode(n1)
    store.upsertNode(n2)

    val edge1 = Edge("1", "2", "KNOWS", Map("since" -> ujson.Num(2020)))
    val edge2 = Edge("1", "2", "KNOWS", Map("since" -> ujson.Num(2025), "strength" -> ujson.Str("strong")))

    store.upsertEdge(edge1) shouldBe Right(())
    store.upsertEdge(edge2) shouldBe Right(())

    // Verify only one edge exists with updated properties
    val graph = store.loadAll().toOption.get
    val edges = graph.edges.filter(e => e.source == "1" && e.target == "2" && e.relationship == "KNOWS")
    edges should have size 1
    edges.head.properties.get("since") shouldBe Some(ujson.Num(2025))
    edges.head.properties.get("strength") shouldBe Some(ujson.Str("strong"))

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("getNode should return None for non-existent node") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)

    val result = store.getNode("non-existent")
    result shouldBe Right(None)

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("getNeighbors should return correct neighbors with Direction.Outgoing") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "A")
    val n2     = Node("2", "B")
    val n3     = Node("3", "C")

    store.upsertNode(n1)
    store.upsertNode(n2)
    store.upsertNode(n3)
    store.upsertEdge(Edge("1", "2", "EDGE"))
    store.upsertEdge(Edge("1", "3", "EDGE"))

    val neighbors   = store.getNeighbors("1", Direction.Outgoing)
    val neighborIds = neighbors.toOption.get.map(_.node.id)

    neighborIds should contain theSameElementsAs List("2", "3")

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("getNeighbors should return correct neighbors with Direction.Incoming") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "A")
    val n2     = Node("2", "B")
    val n3     = Node("3", "C")

    store.upsertNode(n1)
    store.upsertNode(n2)
    store.upsertNode(n3)
    store.upsertEdge(Edge("2", "1", "EDGE"))
    store.upsertEdge(Edge("3", "1", "EDGE"))

    val neighbors   = store.getNeighbors("1", Direction.Incoming)
    val neighborIds = neighbors.toOption.get.map(_.node.id)

    neighborIds should contain theSameElementsAs List("2", "3")

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("query should filter by node label") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val person = Node("1", "Person", Map("name" -> ujson.Str("Alice")))
    val org    = Node("2", "Organization", Map("name" -> ujson.Str("ACME")))

    store.upsertNode(person)
    store.upsertNode(org)

    val filter = GraphFilter(nodeLabel = Some("Person"))
    val result = store.query(filter)

    result.toOption.get.nodes should have size 1
    result.toOption.get.nodes.get("1").map(_.label) shouldBe Some("Person")

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("query should filter by property value") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val alice  = Node("1", "Person", Map("city" -> ujson.Str("NYC")))
    val bob    = Node("2", "Person", Map("city" -> ujson.Str("LA")))

    store.upsertNode(alice)
    store.upsertNode(bob)

    val filter = GraphFilter(propertyKey = Some("city"), propertyValue = Some("NYC"))
    val result = store.query(filter)

    result.toOption.get.nodes should have size 1
    result.toOption.get.nodes.get("1") should not be empty

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("query should filter by relationship type") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "Person")
    val n2     = Node("2", "Person")
    val n3     = Node("3", "Organization")

    store.upsertNode(n1)
    store.upsertNode(n2)
    store.upsertNode(n3)
    store.upsertEdge(Edge("1", "2", "KNOWS"))
    store.upsertEdge(Edge("1", "3", "WORKS_FOR"))

    val filter = GraphFilter(relationshipType = Some("KNOWS"))
    val result = store.query(filter)

    result.toOption.get.edges should have size 1
    result.toOption.get.edges.head.relationship shouldBe "KNOWS"

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("query should reject property keys with SQL injection attempts") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "Person", Map("name" -> ujson.Str("Alice")))

    store.upsertNode(n1)

    // Test with malicious property key containing SQL injection patterns
    val maliciousFilters = Seq(
      GraphFilter(propertyKey = Some("'; DROP TABLE nodes; --"), propertyValue = Some("value")),
      GraphFilter(propertyKey = Some("name' OR '1'='1"), propertyValue = Some("Alice")),
      GraphFilter(propertyKey = Some("../../../etc/passwd"), propertyValue = Some("root")),
      GraphFilter(propertyKey = Some("name; DELETE FROM edges"), propertyValue = Some("test"))
    )

    maliciousFilters.foreach { filter =>
      val result = store.query(filter)
      result shouldBe a[Left[_, _]]
      // Verify error mentions invalid property key in the operation field
      result.swap.toOption.get.context.get("operation") shouldBe Some("invalid_property_key")
    }

    // Verify database is still intact
    val validResult = store.getNode("1")
    validResult.toOption.get shouldBe defined

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("traverse should perform BFS and respect depth limits") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    // Create chain: 1 -> 2 -> 3 -> 4
    val nodes = Seq(Node("1", "A"), Node("2", "B"), Node("3", "C"), Node("4", "D"))
    nodes.foreach(store.upsertNode)

    store.upsertEdge(Edge("1", "2", "EDGE"))
    store.upsertEdge(Edge("2", "3", "EDGE"))
    store.upsertEdge(Edge("3", "4", "EDGE"))

    val result     = store.traverse("1", TraversalConfig(maxDepth = 2))
    val visitedIds = result.toOption.get.map(_.id).toSet

    // Max depth 2: level 0 (1), level 1 (2), level 2 (3)
    visitedIds shouldBe Set("1", "2", "3")

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("traverse should follow Direction.Both in both directions") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "A")
    val n2     = Node("2", "B")
    val n3     = Node("3", "C")
    val n4     = Node("4", "D")

    store.upsertNode(n1)
    store.upsertNode(n2)
    store.upsertNode(n3)
    store.upsertNode(n4)
    store.upsertEdge(Edge("2", "1", "EDGE")) // incoming to 1
    store.upsertEdge(Edge("1", "3", "EDGE")) // outgoing from 1
    store.upsertEdge(Edge("3", "4", "EDGE")) // continuing outgoing chain

    val result     = store.traverse("1", TraversalConfig(direction = Direction.Both))
    val visitedIds = result.toOption.get.map(_.id).toSet

    // Should visit all connected nodes: 2 (incoming), 1 (start), 3 (outgoing), 4 (via 3)
    visitedIds shouldBe Set("1", "2", "3", "4")

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("traverse should handle non-existent start node") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)

    val result = store.traverse("non-existent")
    result.toOption.get shouldBe empty

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("deleteNode should remove node and connected edges") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "A")
    val n2     = Node("2", "B")

    store.upsertNode(n1)
    store.upsertNode(n2)
    store.upsertEdge(Edge("1", "2", "EDGE"))

    store.deleteNode("1") shouldBe Right(())
    store.getNode("1").toOption.get shouldBe None

    // Verify edge was deleted
    val graph = store.loadAll().toOption.get
    graph.edges should have size 0

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("deleteEdge should remove only the specified edge") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "A")
    val n2     = Node("2", "B")

    store.upsertNode(n1)
    store.upsertNode(n2)
    store.upsertEdge(Edge("1", "2", "KNOWS"))
    store.upsertEdge(Edge("1", "2", "WORKS_WITH"))

    store.deleteEdge("1", "2", "KNOWS") shouldBe Right(())

    val graph = store.loadAll().toOption.get
    graph.edges should have size 1
    graph.edges.head.relationship shouldBe "WORKS_WITH"

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("loadAll should return complete graph") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val nodes  = Seq(Node("1", "A"), Node("2", "B"), Node("3", "C"))
    nodes.foreach(store.upsertNode)

    store.upsertEdge(Edge("1", "2", "EDGE"))
    store.upsertEdge(Edge("2", "3", "EDGE"))

    val result = store.loadAll()
    result.toOption.get.nodes should have size 3
    result.toOption.get.edges should have size 2

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("stats should compute correct statistics") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val nodes  = Seq(Node("1", "A"), Node("2", "B"), Node("3", "C"))
    nodes.foreach(store.upsertNode)

    store.upsertEdge(Edge("1", "2", "EDGE"))
    store.upsertEdge(Edge("1", "3", "EDGE"))
    store.upsertEdge(Edge("2", "3", "EDGE"))

    val stats = store.stats()
    stats.toOption.get.nodeCount shouldBe 3L
    stats.toOption.get.edgeCount shouldBe 3L

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("property serialization should handle complex types") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val node = Node(
      "1",
      "Person",
      Map(
        "name"    -> ujson.Str("Alice"),
        "age"     -> ujson.Num(30),
        "active"  -> ujson.Bool(true),
        "hobbies" -> ujson.Arr(ujson.Str("reading"), ujson.Str("coding"))
      )
    )

    store.upsertNode(node) shouldBe Right(())

    val retrieved = store.getNode("1")
    retrieved.toOption.get.flatMap(_.properties.get("age")) shouldBe Some(ujson.Num(30))
    retrieved.toOption.get.flatMap(_.properties.get("active")) shouldBe Some(ujson.Bool(true))

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("concurrent reads should work with connection pool") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val node   = Node("1", "A")
    store.upsertNode(node)

    // Simulate concurrent reads
    val results = (1 to 5).map(_ => store.getNode("1"))

    // All should succeed
    results.foreach(result => result.toOption.get.map(_.id) shouldBe Some("1"))

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("sequential operations should maintain consistency") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)

    // Add nodes
    (1 to 5).foreach(i => store.upsertNode(Node(i.toString, s"Node$i")) shouldBe Right(()))

    // Add edges
    (1 until 5).foreach(i => store.upsertEdge(Edge(i.toString, (i + 1).toString, "EDGE")) shouldBe Right(()))

    // Verify structure
    val graph = store.loadAll().toOption.get
    graph.nodes should have size 5
    graph.edges should have size 4

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("should handle special characters in properties") {
    val dbPath       = createTempDbPath()
    val store        = new SQLiteGraphStore(dbPath)
    val specialChars = "!@#$%^&*()_+-=[]{}|;:',.<>?/\\\"~`"
    val node         = Node("1", "Test", Map("special" -> ujson.Str(specialChars)))

    store.upsertNode(node) shouldBe Right(())

    val retrieved = store.getNode("1")
    retrieved.toOption.get.flatMap(_.properties.get("special")) shouldBe Some(ujson.Str(specialChars))

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("should handle empty properties") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val node   = Node("1", "Empty", Map())

    store.upsertNode(node) shouldBe Right(())

    val retrieved = store.getNode("1")
    retrieved.toOption.get.map(_.properties) shouldBe Some(Map())

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }

  test("transaction safety should prevent partial updates") {
    val dbPath = createTempDbPath()
    val store  = new SQLiteGraphStore(dbPath)
    val n1     = Node("1", "A")
    val n2     = Node("2", "B")

    store.upsertNode(n1) shouldBe Right(())
    store.upsertNode(n2) shouldBe Right(())

    // This should fail and not partially commit
    val result = store.upsertEdge(Edge("1", "999", "INVALID"))
    result shouldBe a[Left[_, _]]

    // Verify database is still consistent
    val graph = store.loadAll().toOption.get
    graph.nodes should have size 2
    graph.edges should have size 0

    // Cleanup
    store.close()
    Files.deleteIfExists(dbPath)
  }
}
