package io.monadplus.concurrency

import cats.effect.{Concurrent, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue

import scala.concurrent.duration._

class Buffering[F[_]](q1: Queue[F, Int], q2: Queue[F, Int])(implicit F: Concurrent[F]) {

  def start: Stream[F, Unit] =
    Stream(
      Stream.range(0, 100000).covary[F].through(q1.enqueue),
      q1.dequeue.through(q2.enqueue),
      q2.dequeue.evalMap(n => F.delay(println(s"Pulling out $n from Queue #2")))
    ).parJoin(3)
}

object Fifo extends IOApp {

  val program = for {
    q1 <- Stream.eval(Queue.bounded[IO, Int](1))
    q2 <- Stream.eval(Queue.bounded[IO, Int](100))
    bp = new Buffering[IO](q1, q2)
    _  <- Stream.sleep_[IO](2.seconds).concurrently(bp.start.drain)
  } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    program.compile.drain.as(ExitCode.Success)
}
