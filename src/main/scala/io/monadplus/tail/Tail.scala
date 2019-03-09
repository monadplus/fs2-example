package io.monadplus.tail

import java.nio.file.{Path, Paths, StandardOpenOption, WatchEvent}
import java.util.concurrent.Executors

import cats._
import cats.data._
import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import fs2._
import fs2.concurrent.{NoneTerminatedQueue, Queue}
import fs2.io.Watcher.Event
import fs2.io.Watcher.Event.Modified
import fs2.io._
import fs2.io.file._

import scala.concurrent.ExecutionContext

/*
watch path events to queue
Read queue, when event is found read latest position read until the end of the file
print it, and termiante
 */
object Tail extends IOApp {
  def watchPath[F[_]: Concurrent, O](path: Path, queue: NoneTerminatedQueue[F, Unit]): Stream[F, Unit] =
    watch(path)
      .map {
        case Modified(_, _) => Some(())
        case _ => None
      }
      .through(queue.enqueue)

  def blockingExecutionContext[F[_]](implicit F: Sync[F]): Stream[F, ExecutionContext] =
    Stream.resource {
      Resource.make(F.delay(ExecutionContext.fromExecutorService(Executors.newCachedThreadPool()))) {
        ec =>
          F.delay(ec.shutdown())
      }
    }

  def loop[F[_]](queue: NoneTerminatedQueue[F, Unit],
                 fileHandle: FileHandle[F],
                 lastRead: Long)(implicit F: Sync[F]): F[Unit] = {
    queue.dequeue1.flatMap {
      case None => F.unit
      case _ => fileHandle.size.flatMap { size =>
        if (lastRead == size)
          loop(queue, fileHandle, lastRead)
        else
          fileHandle.read((size - lastRead).toInt, lastRead).flatMap {
            case Some(c) =>
              Stream.chunk(c).through(text.utf8Decode).covary[F].compile.string.flatMap(s => F.delay(println(s))) >> loop(queue, fileHandle, size)
            case None =>
              loop(queue, fileHandle, lastRead)
          }
      }
    }
  }

  def consumeEvents[F[_]: Sync: ContextShift](path: Path, ec: ExecutionContext, queue: NoneTerminatedQueue[F, Unit]): Stream[F, Unit] = {
    pulls.fromPath(path, ec, List(StandardOpenOption.READ)).map(_.resource).flatMap { fileHandle =>
      Pull.output1(fileHandle)
    }.stream.flatMap { fileHandle =>
      Stream.eval(queue.enqueue1(().some)) ++ Stream.eval(loop(queue, fileHandle, 0L))
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val path: Path = Paths.get("data/tail.txt")
    blockingExecutionContext[IO].flatMap { ec =>
      Stream.eval(Queue.boundedNoneTerminated[IO, Unit](1)).flatMap { queue =>
        consumeEvents[IO](path, ec, queue).concurrently(watchPath(path, queue))
      }
    }.compile.drain.as(ExitCode.Success)
  }
}
