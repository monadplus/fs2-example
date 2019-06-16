package io.monadplus

import cats.effect._
import cats.implicits._
import fs2._

import scala.concurrent.duration._
import scala.reflect.ClassTag

object Intersperse extends IOApp {

  def intersperse[F[_], O, O2 >: O: ClassTag](separator: O2): Pipe[F, O, O2] =
    (_: Stream[F, O]).pull.echo1.flatMap {
      case None => Pull.done
      case Some(s) =>
        s.repeatPull {
            _.uncons.flatMap {
              case None => Pull.pure(None)
              case Some((hd, tl)) =>
                val size = hd.size * 2
                val buffer = new Array[O2](size)
                var i = 0
                while (i < hd.size) {
                  buffer(i * 2) = separator
                  buffer(i * 2 + 1) = hd(i)
                  i += 1
                }
                Pull.output(Chunk.array(buffer)).as(Some(tl))
            }
          }
          .pull
          .echo
    }.stream

  def program[F[_]: Sync: Timer]: Stream[F, Unit] =
    Stream
      .awakeEvery[F](100.millis)
      .zipWithIndex
      .map(_._2)
      .through(intersperse[F, Long, Long](-1L))
      .evalMap(v => Sync[F].delay(println(v)))

  override def run(args: List[String]): IO[ExitCode] =
    program[IO].compile.drain.as(ExitCode.Success)
}
