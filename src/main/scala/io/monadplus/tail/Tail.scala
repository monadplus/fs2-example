package io.monadplus.tail

import java.nio.file._
import java.util.concurrent.Executors

import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2._
import fs2.io.file._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Tail extends IOApp {

  def blockingExecutionContext[F[_]](implicit F: Sync[F]): Stream[F, ExecutionContext] =
    Stream.resource {
      Resource.make(F.delay(ExecutionContext.fromExecutorService(Executors.newCachedThreadPool()))) {
        ec =>
          F.delay(ec.shutdown())
      }
    }

  // TODO FileHandle is not aware of changes when  you open and rewrite the file.
  def watchFile[F[_]: Concurrent](path: Path,
                                  fileHandle: FileHandle[F],
                                  interrupt: Ref[F, Boolean],
                                  pollTime: FiniteDuration = 100.millis)(
      implicit F: Sync[F],
      timer: Timer[F]): Stream[F, Unit] = {

    def loop(lastRead: Long): F[Unit] = {
      def delay[A](fa: F[A]): F[A] =
        timer.sleep(pollTime) >> fa

      def size: F[Long] =
        F.delay(Files.size(path))

      interrupt.get.flatMap { isFinished =>
        if (isFinished)
          F.unit
        else
          size.flatMap { size =>
            if (lastRead >= size) {
              delay(loop(lastRead))
            } else {
              fileHandle
                .read((size - lastRead).toInt, lastRead)
                .flatMap {
                  case Some(c) =>
                    Stream
                      .chunk(c)
                      .through(text.utf8Decode)
                      .covary[F]
                      .compile
                      .string
                      .flatMap(s => F.delay(print(s))) >> delay(loop(size))
                  case None =>
                    println("None")
                    delay(loop(lastRead))
                }
            }
          }
      }
    }

    Stream.eval(loop(0L).handleErrorWith(e => F.delay(println(s"Error: ${e.getMessage}"))))
  }

  def fileHandle[F[_]: Sync: ContextShift](path: Path,
                                           blockingEC: ExecutionContext): Stream[F, FileHandle[F]] =
    pulls
      .fromPath(path, blockingEC, List(StandardOpenOption.READ))
      .flatMap { cancellable =>
        Pull.output1(cancellable.resource)
      }
      .stream

  def watchStdIn[F[_]: ContextShift](blockingEC: ExecutionContext, interrupt: Ref[F, Boolean])(
      implicit F: Sync[F]): Stream[F, Unit] =
    io.stdin(256 * 1024, blockingEC)
      .through(text.utf8Decode)
      .evalMap[F, Option[Unit]] { s =>
        if (s.trim == "finish")
          interrupt.modify(_ => true -> None)
        else
          F.pure(().some)
      }
      .unNoneTerminate // This is redundant

  def program[F[_]: Concurrent: Timer: ContextShift]: Stream[F, Unit] =
    blockingExecutionContext.flatMap { blockingExecutionContext =>
      Stream.eval(Ref.of[F, Boolean](false)).flatMap { interrupt =>
        val path: Path = Paths.get("data/tail.txt")
        fileHandle(path, blockingExecutionContext).flatMap { fileHandle =>
          watchFile(path, fileHandle, interrupt).concurrently(
            watchStdIn(blockingExecutionContext, interrupt))
        }
      }
    }

  override def run(args: List[String]): IO[ExitCode] =
    program[IO].compile.drain.as(ExitCode.Success)
}
