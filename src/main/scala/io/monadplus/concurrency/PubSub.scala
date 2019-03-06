package io.monadplus.concurrency

import scala.concurrent.duration._
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Timer}
import cats.syntax.all._
import fs2.{Pipe, Stream}
import fs2.concurrent.{SignallingRef, Topic}

/*
  - Subscriber #1 should receive 15 events + the initial empty event
  - Subscriber #2 should receive 10 events
  - Subscriber #3 should receive 5 events
  - Publisher sends Quit event
  - Subscribers raise interrupt signal on Quit event
 */

sealed trait Event
case class Text(value: String) extends Event
case object Quit               extends Event

class EventService[F[_]](eventsTopic: Topic[F, Event], interrupter: SignallingRef[F, Boolean])(
    implicit F: Concurrent[F],
    timer: Timer[F]
) {

  def startPublisher: Stream[F, Unit] = {
    val textEvents = eventsTopic.publish(
      Stream
        .awakeEvery[F](1.second)
        .zipRight(Stream(Text(System.currentTimeMillis().toString)).repeat)
    )
    val quitEvent = Stream.eval(eventsTopic.publish1(Quit))

    (textEvents.take(15) ++ quitEvent ++ textEvents).interruptWhen(interrupter)
  }

  def startSubscribers: Stream[F, Unit] = {
    val s1: Stream[F, Event] = eventsTopic.subscribe(10)
    val s2: Stream[F, Event] = Stream.sleep_[F](5.seconds) ++ eventsTopic.subscribe(10)
    val s3: Stream[F, Event] = Stream.sleep_[F](10.seconds) ++ eventsTopic.subscribe(10)

    def sink(subscriberNumber: Int): Pipe[F, Event, Unit] =
      _.flatMap {
        case e @ Text(_) =>
          Stream.eval(F.delay(println(s"Subscriber #$subscriberNumber processing event: $e")))
        case Quit => Stream.eval(interrupter.set(true))
      }

    Stream(s1.through(sink(1)), s2.through(sink(2)), s3.through(sink(3))).parJoin(3)
  }
}

object PubSub extends IOApp {

  val program = for {
    topic   <- Stream.eval(Topic[IO, Event](Text("Initial Event")))
    signal  <- Stream.eval(SignallingRef[IO, Boolean](false))
    service = new EventService[IO](topic, signal)
    _       <- service.startPublisher.concurrently(service.startSubscribers).drain
  } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    program.compile.drain.as(ExitCode.Success)
}
