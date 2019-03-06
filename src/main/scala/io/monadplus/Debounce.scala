package io.monadplus

import cats.implicits._
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Timer}
import fs2._
import fs2.concurrent.Queue

import scala.concurrent.duration.FiniteDuration

object Debounce {

  class StreamOps[F[_], O](self: Stream[F, O]) {
    def debounce[F2[x] >: F[x]](d: FiniteDuration)(implicit F: Concurrent[F2],
                                                   timer: Timer[F2]): Stream[F2, O] =
      Stream.eval(Queue.bounded[F2, Option[O]](1)).flatMap { queue =>
        Stream.eval(Ref.of[F2, Option[O]](None)).flatMap { ref =>
          def enqueueLatest: F2[Unit] = ref.modify(s => None -> s).flatMap {
            case v @ Some(_) => queue.enqueue1(v) // enqueue the element and release it
            case None        => F.unit
          }

          def onChunk(ch: Chunk[O]): F2[Unit] =
            if (ch.isEmpty) F.unit
            else
              ref.modify(s => Some(ch(ch.size - 1)) -> s).flatMap {
                case None =>
                  F.start(timer.sleep(d) >> enqueueLatest)
                    .void // holds an element until d units of time has passed and then enqueue it
                case Some(_) =>
                  F.unit // there is an element on hold, enqueueLatest will release it.
              }

          val in: Stream[F2, Unit] = self.chunks.evalMap(onChunk) ++
            Stream.eval_(enqueueLatest >> queue.enqueue1(None)) // enqueueLatest latest element and finish

          val out: Stream[F2, O] = queue.dequeue.unNoneTerminate // output all elements enqueued by in until in ends its elements and emits a None

          out.concurrently(in)
        }
      }
  }

}
