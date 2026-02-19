package org.llm4s.knowledgegraph.storage

import org.llm4s.error.LLMError
import org.llm4s.knowledgegraph.Node
import org.llm4s.types.Result

private[storage] object GraphTraversal {

  def bfs(
    startId: String,
    config: TraversalConfig
  )(
    getNode: String => Result[Option[Node]],
    getNeighborIds: (String, Direction) => Result[Seq[String]]
  ): Result[Seq[Node]] =
    getNode(startId).flatMap {
      case None =>
        Right(Seq.empty)
      case Some(_) =>
        var visited                 = Set.empty[String]
        var queue                   = scala.collection.immutable.Queue((startId, 0))
        val result                  = scala.collection.mutable.ListBuffer.empty[Node]
        var error: Option[LLMError] = None

        while (queue.nonEmpty && error.isEmpty) {
          val ((currentNodeId, depth), newQueue) = queue.dequeue
          queue = newQueue

          if (!visited.contains(currentNodeId) && !config.visitedNodeIds.contains(currentNodeId)) {
            getNode(currentNodeId) match {
              case Left(err) =>
                error = Some(err)
              case Right(None) =>
              case Right(Some(currentNode)) =>
                visited += currentNodeId
                result += currentNode

                if (depth < config.maxDepth) {
                  getNeighborIds(currentNodeId, config.direction) match {
                    case Left(err) =>
                      error = Some(err)
                    case Right(nextIds) =>
                      nextIds.foreach { nextId =>
                        if (!visited.contains(nextId) && !config.visitedNodeIds.contains(nextId)) {
                          queue = queue.enqueue((nextId, depth + 1))
                        }
                      }
                  }
                }
            }
          }
        }

        error match {
          case Some(err) => Left(err)
          case None      => Right(result.toSeq)
        }
    }
}
