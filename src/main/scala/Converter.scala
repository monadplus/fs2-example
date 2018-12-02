import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import fs2.{Stream, io, text}
import java.nio.file.{Path, Paths}
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

object Converter extends IOApp {

  private def fahrenheitToCelsius(f: Double): Double =
    (f - 32.0) * (5.0 / 9.0)

  private val blockingExecutionContext =
    Resource.make(IO(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))))(
      ec => IO(ec.shutdown())
    )

  def converter(input: Path, output: Path): Stream[IO, Unit] =
    Stream.resource(blockingExecutionContext).flatMap { blockingEC =>
      io.file
        .readAll[IO](input, blockingEC, 4096)
        .through(text.utf8Decode)
        .through(text.lines)
        .filter(s => !s.trim.isEmpty && !s.startsWith("//"))
        .map(line => fahrenheitToCelsius(line.toDouble).toString)
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(io.file.writeAll(output, blockingEC))
    }

  def run(args: List[String]): IO[ExitCode] = {
    val in  = Paths.get("data/fahrenheit.txt")
    val out = Paths.get("data/celsius.txt")
    converter(in, out).compile.drain.as(ExitCode.Success)
  }
}
