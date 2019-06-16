package io.monadplus

import cats.effect.{Concurrent, IO}
import cats.free.Free
import fs2.{Chunk, Pipe, Stream}
import fs2.concurrent.Queue

object practise {
  def prefetchN2[F[_]: Concurrent, O](n: Int): Pipe[F, O, Chunk[O]] =
    in =>
      Stream.eval(Queue.bounded[F, Option[Chunk[O]]](n)).flatMap { queue =>
        queue.dequeue.unNoneTerminate
          .mapChunks(c => Chunk.concat(c.toVector))
          .chunks
          .concurrently(in.chunks.noneTerminate.through(queue.enqueue))
    }

}

object Playground {
  ???
}
