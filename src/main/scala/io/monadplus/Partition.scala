package io.monadplus

import cats._, cats.data._, cats.implicits._
import cats.effect._, cats.effect.implicits._
import fs2._, fs2.io._

import scala.concurrent.duration._
import fs2.concurrent.Queue
import cats.effect.concurrent.Ref

object Partitions extends IOApp {
  def partitions[F[_]: Sync, A, K](selector: A => F[K]): Pipe[F, A, (K, Stream[F, A])] =
    in =>
      // Stream[F, (K, Stream[F, A]]
      Stream.eval(Ref.of[F, Map[K, Queue[F, Option[A]]]](Map.empty)).flatMap { st =>
        val cleanup: F[Unit] = {
          import alleycats.std.std._
          st.get.flatMap(_.traverse_(_.enqueue1(None)))
        }

        (in ++ Stream.eval_(cleanup))
          .evalMap { e1 =>
            (selector(e1), st.get).mapN { (key, queues) =>
              queues
                .get(key)
                .fold {
                  for {
                    newQ <- Queue.unbounded[F, Option[A]]
                    _ <- st.modify(_ + (key -> newQ))
                    _ <- newQ.enqueue1(el.some)
                  } yield (key -> newQ.dequeue.unNoneTerminate).some
                }(_.enqueue1(e1.some))
            }.flatten
          }
          .onFinalize(cleanup)
      }

  def selector(i: Int): IO[Int] =
    IO.pure(i % 3)

  def flakiness[A]: Pipe[IO, A, A] = in => {
    def wait = IO(scala.util.Random.nextInt(500)).flatMap(d => Timer[IO].sleep(d.millis))

    Stream.repeatEval(wait).zipRight(in)
  }

  def run(args: List[String]): IO[ExitCode] =
    Stream
      .range(1, 100)
      .covary[IO]
      .through(partitions(selector))
      .map {
        case (k, st) => st.tupleLeft(k).through(flakiness).through(Sink.showLinesStdOut)
      }
      .parJoinUnbounded
      .compile
      .drain
      .as(ExitCode.Success)

}

object Memoize extends IOApp {
  // Ref + Def, (ref.of Option + def Either throwable A
  def memoize[F[_], A](f: F[A])(implicit F: Async[F]): F[F[A]] =
    ???

  def run(args: List[String]): IO[ExitCode] =
    IO.pure(ExitCode.Success)
}
