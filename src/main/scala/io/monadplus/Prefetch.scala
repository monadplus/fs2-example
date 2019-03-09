package io.monadplus

import scala.concurrent.duration._

import fs2._
import fs2.concurrent.Queue

import cats.implicits._
import cats.effect._

object Prefetch extends IOApp {

  def prefetch[F[_], F2[x] >: F[x]: Concurrent, O](s: Stream[F, O])(n: Int): Stream[F2, O] =
    prefetchN[F, F2, O](s)(1)

  def prefetchN[F[_], F2[x] >: F[x]: Concurrent, O](s: Stream[F, O])(n: Int): Stream[F2, O] =
    Stream.eval(Queue.bounded[F2, Option[Chunk[O]]](n)).flatMap { queue =>
      queue.dequeue.unNoneTerminate
        .flatMap(Stream.chunk)
        .concurrently(s.chunks.noneTerminate.covary[F2].through(queue.enqueue))
    }

  val stream =
    Stream
      .awakeEvery[IO](100.millis)
      .zipWithIndex
      .buffer(5)
//      .prefetchN(5) // wont wait
      .evalMap {
        case (_, index) =>
          IO(println(s"index: $index"))
      }
      .interruptAfter(5.seconds)

  override def run(args: List[String]): IO[ExitCode] =
    stream.compile.drain.as(ExitCode.Success)
}
