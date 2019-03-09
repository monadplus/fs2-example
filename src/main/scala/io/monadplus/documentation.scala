package io.monadplus

import java.util.concurrent.Executors

import cats.implicits._
import cats.effect._
import fs2._
import fs2.concurrent.Queue

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/*
### Streams can terminate:
  - When they are finite and they hit their last element
  - with an error
  - when built with an interrupting combinator like interruptWhen or mergeHalt or concurrently
  - Whenever you want, using a Pull with Pull.done
  - never

### fs2 vs akka-streams:
 - akka-streams uses Future, which is worse than IO in about every way we care about
 - akka-streams are way more complicated than fs2, so it's harder to learn
 - We want to use fs2 for itself and its integrations, and we don't want engineers here to learn two streaming systems
 - Any integrations that are akka-streams only and don't exist for fs2 we can just hijack using streamz-converter and make into fs2 streams

 */
object documentation extends App {

  // Useful: import fs2.io._

  val empty = Stream.empty
  Stream.emit(1)
  Stream(1, 2, 3)
  Stream.emits(List(1, 2, 3))
  (Stream(1, 2, 3) ++ Stream(4, 5)).toList // ++ takes constant time
  Stream(1, 2, 3).map(_ + 1).toList
  Stream(1, 2, 3).filter(_ % 2 != 0).toList
  Stream(1, 2, 3).fold(0)(_ + _).toList
  Stream(none, 2.some, 3.some).collect { case Some(i) => i }.toList
  Stream.range(0, 5).intersperse(42).toList
  Stream(1, 2, 3).flatMap(i => Stream(i, i)).toList // List(1,1,2,2,3,3)
  Stream(1, 2, 3).repeat.take(9).toList // List(1,2,3,1,2,3,1,2,3)

  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
  implicit val timer: Timer[IO] = IO.timer(ec)
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  val s1err = Stream.raiseError[IO](new Exception("BOOM !"))
  s1err.attempt
  s1err.handleErrorWith(e => Stream.emit(e.getMessage)).compile.toList.unsafeRunSync()
  val eff = Stream.eval(IO { println("BEING RUN!!"); 1 + 1 })
  Stream.attemptEval(IO.raiseError(new Exception("BOOM !")))
  eff.compile.toList.unsafeRunSync()
  eff.compile.drain //F[Unit]
  eff.compile.fold(0)(_ + _).unsafeRunSync()

  // Chunks:
  //    A strict, finite sequence of values that supports
  //    efficient indexed based lookup of elements.
  val s1c: Stream[Pure, Double] = Stream.chunk(Chunk.doubles(Array(1.0, 2.0, 3.0)))
  s1c.mapChunks { ds =>
    /* do things*/
    ds.toDoubles
  }

  val acquire: IO[Int] = ???
  val release: IO[Unit] = ???
  Stream
    .bracket(acquire)(_ => release)
    .compile
    .drain
    .unsafeRunSync()

  // *** Exercises ***

  def repeat[F[_], O](s: Stream[F, O]): Stream[F, O] =
    s ++ repeat(s) // trick: ++ is called by name

  def drain[F[_], O](s: Stream[F, O]): Stream[F, INothing] =
    s.mapChunks(_ => Chunk.empty)

  def attempt[F[_]: RaiseThrowable, O](s: Stream[F, O]): Stream[F, Either[Throwable, O]] =
    s.map(Right(_)).handleErrorWith(t => Stream.emit(Left(t)))

  // *** Pull[F[_], O, R] ***

  // Using chunksOpt
  def tk[F[_], O](n: Long): Pipe[F, O, O] =
    is =>
      is.scanChunksOpt(n) { n =>
        if (n < 0) None
        else {
          Some { c =>
            c.size match {
              case m if m <= n => (n - m, c)
              case _           => (0, c.take(n.toInt))
            }
          }
        }
    }

  def tk2[F[_], O](n: Long): Pipe[F, O, O] = {
    def go(s: Stream[F, O], n: Long): Pull[F, O, Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) =>
          hd.size match {
            case m if m <= n => Pull.output(hd) >> go(tl, n - m)
            case _           => Pull.output(hd.take(n.toInt)) >> Pull.done
          }
        case None => Pull.done
      }
    in =>
      go(in, n).stream
  }

  Stream(1, 2, 3, 4).through(tk(2)).toList
  Stream(1, 2, 3, 4).through(tk2(2)).toList

  // *** Exercises ***

  def takeWhile[F[_], O](p: O => Boolean): Pipe[F, O, O] = {
    def go(s: Stream[F, O]): Pull[F, O, Option[Stream[F, O]]] =
      s.pull.uncons.flatMap {
        case None => Pull.pure(none)
        case Some((hd, tl)) =>
          hd.indexWhere(o => !p(o)) match {
            case None => Pull.output(hd) >> go(tl)
            case Some(idx) =>
              val (pfx, sfx) = hd.splitAt(idx)
              Pull.output(pfx).as(tl.cons(sfx).some)
          }
      }
    in =>
      go(in).stream
  }

  def fold[F[_], O, O2](z: O2)(f: (O2, O) => O2): Pipe[F, O, O2] = {
    def fold_(s: Stream[F, O])(z: O2): Pull[F, INothing, O2] =
      s.pull.uncons.flatMap {
        case None => Pull.pure(z)
        case Some((hd, tl)) =>
          val acc = hd.foldLeft(z)(f)
          fold_(tl)(acc)
      }

    is =>
      fold_(is)(z).flatMap(Pull.output1).stream
  }

  def scan[F[_], O, O2](z: O2)(f: (O2, O) => O2): Pipe[F, O, O2] = {
    def scan_(s: Stream[F, O])(o2: O2): Pull[F, O2, Unit] =
      s.pull.uncons1.flatMap {
        case None => Pull.done
        case Some((hd, tl)) =>
          val next = f(o2, hd)
          Pull.output1(next) >> scan_(tl)(next)
      }

    is =>
      (Pull.output1(z) >> scan_(is)(z)).stream
  }

//  Is it possible to implement this ?

//  def scan2[F[_], O, O2](z: O2)(f: (O2, O) => O2): Pipe[F, O, O2] = {
//    def scan_(s: Stream[F, O])(o2: O2): Stream[F, O2] =
//      s.repeatPull {
//        _.uncons1.flatMap {
//          case None => Pull.pure(None)
//          case Some((hd, tl)) =>
//            val next = f(o2, hd)
//            Pull.output1(next).as(Some(tl))
//        }
//      }
//    is =>
//      Pull.output1(z).stream ++ scan_(is)(z)
//  }

  // *** Concurrency ***

  // merge, concurrently, interrupt, either, mergeHaltBoth, parJoin

  Stream(1, 2, 3)
    .merge(Stream.eval(IO.sleep(100.millis) >> IO.pure(4)))
    .compile
    .toList
    .unsafeRunSync

  val data: Stream[IO, Int] = Stream.range(1, 10).covary[IO]
  Stream
    .eval(fs2.concurrent.SignallingRef[IO, Int](0))
    .flatMap(s => Stream(s).concurrently(data.evalMap(s.set)))
    .flatMap(_.discrete)
    .takeWhile(_ < 9, takeFailure = true)
    .compile
    .last
    .unsafeRunSync

  // *** Exercises ***

  type Pipe2[F[_], -I, -I2, +O] = (Stream[F, I], Stream[F, I2]) => Stream[F, O]

  def noneTerminate[F[_], O](s: Stream[F, O]): Stream[F, Option[O]] =
    s.map(Some(_)) ++ Stream.emit(None)

  def unNoneTerminate[F[_], O](s: Stream[F, Option[O]]): Stream[F, O] =
    s.repeatPull {
      _.uncons.flatMap {
        case None =>
          Pull.pure(None)
        case Some((hd, tl)) =>
          hd.indexWhere(_.isEmpty) match {
            case Some(0) => Pull.pure(None)
            /* Get all before None*/
            case Some(idx) => Pull.output(hd.take(idx).map(_.get)).as(None)
            /* Output all*/
            case None => Pull.output(hd.map(_.get)).as(Some(tl))
          }
      }
    }

  /** Like `merge`, but halts as soon as _either_ branch halts. */
  def mergeHaltBoth[F[_]: Concurrent, O]: Pipe2[F, O, O, O] =
    (s1, s2) => unNoneTerminate(noneTerminate(s1).merge(noneTerminate(s2)))

  def mergeHaltL[F[_]: Concurrent, O]: Pipe2[F, O, O, O] =
    (s1, s2) => s1.noneTerminate.merge(s2.map(Some(_))).unNoneTerminate

  def mergeHaltR[F[_]: Concurrent, O]: Pipe2[F, O, O, O] =
    (s1, s2) => mergeHaltL[F, O].apply(s2, s1)

  // <-o->
  Stream(1, 2, 3, 4, 5)
    .broadcast[IO]
    .map { worker =>
      worker.evalMap(i => IO(println(s"Processing: $i")))
    }
    .take(3)
    .parJoinUnbounded[IO, Unit]
    .compile
    .drain
    .unsafeRunSync

  // -----------------------------------
  // -----------------------------------
  // -----------------------------------

  // **** Asynchronous effects (cb once) ****
  trait Connection {
    def readBytes(onSuccess: Array[Byte] => Unit, onFailure: Throwable => Unit): Unit
    def readBytesE(onComplete: Either[Throwable, Array[Byte]] => Unit): Unit =
      readBytes(bs => onComplete(Right(bs)), e => onComplete(Left(e)))
    override def toString = "<connection>"
  }

  val c = new Connection {
    override def readBytes(onSuccess: Array[Byte] => Unit, onFailure: Throwable => Unit): Unit = {
      Thread.sleep(200)
      onSuccess(Array(1, 2, 3))
    }
  }

  val bytes = IO.async[Array[Byte]] { cb =>
    c.readBytesE(cb)
  }

  Stream.eval(bytes).map(_.toList).compile.toVector.unsafeRunSync()

  // **** Asynchronous effects (cb invoked multiple times) ****
  type Row = List[String]

  trait CSVHandle {
    def withRows(cb: Either[Throwable, Row] => Unit): Unit
  }

  def rows[F[_]](h: CSVHandle)(implicit F: ConcurrentEffect[F]): Stream[F, Row] =
    for {
      q <- Stream.eval(Queue.unbounded[F, Either[Throwable, Row]])
      _ <- Stream.eval {
        F.delay(h.withRows(e => F.runAsync(q.enqueue1(e))(_ => IO.unit).unsafeRunSync))
      }
      row <- q.dequeue.rethrow
    } yield row

  // *** Reactive Streams *** http://www.reactive-streams.org/

  /*
  Any reactive streams system can interoperate with any other reactive streams system
  by exposing an org.reactivestreams.Publisher or an org.reactivestreams.Subscriber.

  The reactive-streams library provides instances of reactive streams compliant
  publishers and subscribers to ease interoperability with other streaming libraries.
   */

  import fs2.interop.reactivestreams._

  // org.reactivestreams.Publisher (must have a single subscriber only)
  val publisher: StreamUnicastPublisher[IO, Int] = Stream(1, 2, 3).covary[IO].toUnicastPublisher()
  publisher.toStream[IO]

  /*   In FS2, a stream can terminate in one of three ways:

    - Normal input exhaustion. For instance, the stream Stream(1,2,3) terminates after the single chunk (containing the values 1, 2, 3) is emitted.

    - An uncaught exception. For instance, the stream Stream(1,2,3) ++ (throw Err) terminates with Err after the single chunk is emitted.

    - Interruption by the stream consumer. Interruption can be synchronous, as in (Stream(1) ++ (throw Err)) take 1, which will deterministically
     halt the stream before the ++, or it can be asynchronous, as in s1 merge s2 take 3.
     A stream will never be interrupted while it is acquiring a resource (via bracket) or while it is releasing a resource. The bracket function
     guarantees that if FS2 starts acquiring the resource, the corresponding release action will be run.
      Other than that, Streams can be interrupted in between any two ‘steps’ of the stream. The steps themselves are atomic from the perspective of
     FS2. Stream.eval(eff) is a single step, Stream.emit(1) is a single step, Stream(1,2,3) is a single step (emitting a chunk), and
     all other operations (like handleErrorWith, ++, and flatMap) are multiple steps and can be interrupted.
     But importantly, user-provided effects that are passed to eval are never interrupted once they are started (and FS2 does not have enough knowledge of user-provided effects to know how to interrupt them anyway).
   */

  // The take 1 uses Pull but doesn’t examine the entire stream
  case object Err extends Throwable
  (Stream(1) ++ (throw Err)).take(1).toList // List(1)
  (Stream(1) ++ Stream.raiseError[IO](Err)).take(1).compile.toList.unsafeRunSync() // List(1)

  // bracket or onFinalize to clean up resources
  Stream(1)
    .covary[IO]
    .onFinalize(IO { println("finalized!") })
    .take(1)
    .compile
    .toVector
    .unsafeRunSync()

  // Non-deterministic
  val s1 = (Stream(1) ++ Stream(2)).covary[IO]
  val s2 = (Stream.empty ++ Stream.raiseError[IO](Err)).handleErrorWith { e =>
    println(e); Stream.raiseError[IO](e)
  }

  // Merge is non-deterministic !

  /*
    Option 1) s1 may complete before the error in s2 is encountered, in which case nothing will be printed and no error will occur.
    Option 2) s2 may encounter the error before any of s1 is emitted. When the error is reraised by s2, that will terminate the merge
              and asynchronously interrupt s1, and the take terminates with that same error.
    Option 3) s2 may encounter the error before any of s1 is emitted, but during the period where the value
              is caught by handleErrorWith, s1 may emit a value and the take(1) may terminate, triggering interruption of both s1 and s2, before the
              error is reraised but after the exception is printed! In this case, the stream will still terminate without error.
   */

  s1.merge(s2).take(1)
}
