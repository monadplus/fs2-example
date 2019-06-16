package io.monadplus

import java.nio.charset.Charset
import java.nio.file.{Path, Paths}
import java.util.concurrent.Executors

import cats.implicits._
import cats.effect._
import fs2._
import fs2.io._

import scala.concurrent.ExecutionContext

/*
 How to interpret a utf8 unicode character.
 - Intersperse can be represented as 1,2,3,4 bytes.
 - This is called multi-byte sequences.
 - The length is encoded in the higher bits of each octal

 For example:
   01xxxxxx = ASCII (1 byte unicode)
   10xxxxxx = Continuation of a variable length unicode
   110xxxxx 10xxxxxx = 2 bytes unicode
   1110xxxxx 10xxxxxx 10xxxxxx = 3 bytes unicode
   1111xxxxx 10xxxxxx 10xxxxxx 10xxxxxx = 4 bytes unicode
 */

object Utf8 extends IOApp {

  private val utf8Charset = Charset.forName("UTF-8")

  def utf8DecodeC[F[_]]: Pipe[F, Chunk[Byte], String] = {
    def continuationBytes(b: Byte): Int =
      if ((b & 0x80) == 0x00) 0 // ASCII byte
      else if ((b & 0xE0) == 0xC0) 1 // leading byte of a 2 byte seq
      else if ((b & 0xF0) == 0xE0) 2 // leading byte of a 3 byte seq
      else if ((b & 0xF8) == 0xF0) 3 // leading byte of a 4 byte seq
      else -1 // continuation byte or garbage

    def lastIncompleteBytes(bs: Array[Byte]): Int =
      bs.drop(Math.max(0, bs.length - 3)) // Faster than bs.reverse.take(3)
        .reverseIterator
        .map(continuationBytes)
        .zipWithIndex
        .find { case (c, _) => c >= 0 } // TODO: > or >=
        .map {
          case (c, i) => if (c == i) 0 /*the multi-byte fits in the remaining space*/ else i + 1 /*0 based index so + 1*/
        }
        .getOrElse(0)

    def processSingleChunk(outputAndBuffer: (List[String], Chunk[Byte]),
                           nextBytes: Chunk[Byte]): (List[String], Chunk[Byte]) = {
      val (output, buffer) = outputAndBuffer
      val allBytes = Array.concat(buffer.toArray, nextBytes.toArray)
      val splitAt = allBytes.length - lastIncompleteBytes(allBytes)
      if (splitAt == allBytes.length)
        (new String(allBytes, utf8Charset) :: output, Chunk.empty)
      else if (splitAt == 0)
        (output, Chunk.bytes(allBytes))
      else
        (new String(allBytes.take(splitAt), utf8Charset) :: output,
         Chunk.bytes(allBytes.drop(splitAt)))
    }

    def doPull(
        buf: Chunk[Byte],
        s: Stream[Pure, Chunk[Byte]]
    ): Pull[Pure, String, Option[Stream[Pure, Chunk[Byte]]]] =
      s.pull.uncons.flatMap {
        case Some((byteChunks, tl)) =>
          val (output, nextBuffer) =
            byteChunks.foldLeft((Nil: List[String], buf))(processSingleChunk)
          Pull.output(Chunk.seq(output.reverse)) >> doPull(nextBuffer, tl)
        case None if !buf.isEmpty =>
          Pull.output1(new String(buf.toArray, utf8Charset)).as(None)
        case None =>
          Pull.pure(None)
      }

    in: Stream[Pure, Chunk[Byte]] =>
      doPull(Chunk.empty, in).stream
  }

  def utf8Decode[F[_]]: Pipe[F, Byte, String] =
    _.chunks.through(utf8DecodeC)

  def blockingExecutionContext[F[_]: Sync]: Stream[F, ExecutionContext] =
    Stream.resource(
      Resource.make(
        Sync[F].delay(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4)))
      )(ec => Sync[F].delay(ec.shutdown()))
    )

  def program[F[_]: Sync: ContextShift](p: Path): Stream[F, Unit] =
    blockingExecutionContext.flatMap { ec =>
      file
        .readAll(p, ec, 4096)
        .through(utf8Decode)
        .evalMap { s =>
          Sync[F].delay(print(s))
        }
    }

  override def run(args: List[String]): IO[ExitCode] =
    program[IO](Paths.get("data/text2.txt")).compile.drain.as(ExitCode.Success)
}
