package io.monadplus

import cats.implicits._
import cats.effect._
import fs2._

import scala.concurrent.duration._
import fs2.concurrent.Queue
import cats.effect.concurrent.Ref

// Source code from: https://gist.github.com/SystemFw/168ff694eecf45a8d0b93ce7ef060cfd
object Partitions {
  def partitions[F[_]: Concurrent, A, K](selector: A => F[K]): Pipe[F, A, (K, Stream[F, A])] =
    in =>
      Stream.eval(Ref.of[F, Map[K, Queue[F, Option[A]]]](Map.empty)).flatMap { st =>
        val cleanup: F[Unit] = {
          import alleycats.std.all._
          st.get.flatMap(_.traverse_(_.enqueue1(None)))
        }

        // Although the ++ cleanup seems redundant, it is not.
        // Otherwise the queues will never end.
        (in ++ Stream.eval_(cleanup)).evalMap { el =>
            (selector(el), st.get).mapN { (key, queues) =>
              queues
                .get(key)
                .fold {
                  for {
                    newQ <- Queue.unbounded[F, Option[A]]
                    _ <- st.update(_ + (key -> newQ))
                    _ <- newQ.enqueue1(el.some)
                  } yield (key -> newQ.dequeue.unNoneTerminate).some
                }(_.enqueue1(el.some).as(None))
            }.flatten
          }
          .unNone
          .onFinalize(cleanup)
      }

}

object PartitionsTest extends IOApp {

  import Partitions._

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
        case (k, st) => st.tupleLeft(k).through(flakiness).showLinesStdOut
      }
      .parJoinUnbounded
      .compile
      .drain
      .as(ExitCode.Success)

}

