import cats.effect._, cats.effect.implicits._
import cats.implicits._
import fs2.Stream
import scala.concurrent.duration._

object AwakeEvery extends IOApp {

  val io0 =
    Stream
      .awakeEvery[IO](250.millis)
      .take(5)
      .compile
      .toVector
      .map(
        _.foreach(duration => scala.Console.println(duration.toString()))
      )
      .as(ExitCode.Success)

  val ticks =
    Stream
      .awakeEvery[IO](250.millis)
//      Dont: you end up with Stream[IO, IO[...]]
//      .map { i => IO.delay(println(s"Time: $i"))}
      .evalMap[IO, Unit] { i =>
        IO.delay(println(s"Time: $i"))
      }

  val io1: Stream[IO, Unit] =
    ticks.interruptWhen(Stream.eval(IO.sleep(2.second) *> IO { true }))

  // This is better because Stream.sleep is abstracting F
  val io2: Stream[IO, Unit] =
    ticks.interruptWhen(Stream.sleep(2.second).map { _ =>
      true
    })

  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- io2.compile.drain
    } yield ExitCode.Success

}
