package io.monadplus

import java.util.concurrent.Executors

import cats.implicits._
import cats.effect._
import fs2._
import fs2.concurrent.Queue

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Documentation extends App {

  // Useful: import fs2.io._

  val empty = Stream.empty
  Stream.emit(1)
  Stream(1, 2, 3)
  Stream.emits(List(1, 2, 3))
  (Stream(1, 2, 3) ++ Stream(4, 5)).toList // ++ takes constant time
  Stream(1, 2, 3).map(_ + 1).toList
  Stream(1, 2, 3).filter(_ % 2 != 0).toList
  Stream(1, 2, 3).fold(0)(_ + _).toList // List(6)
  Stream(none, 2.some, 3.some).collect { case Some(i) => i }.toList
  Stream.range(0, 5).intersperse(42).toList
  Stream(1, 2, 3).flatMap(i => Stream(i, i)).toList // List(1,1,2,2,3,3)
  Stream(1, 2, 3).repeat.take(9).toList             // List(1,2,3,1,2,3,1,2,3)

  implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
  implicit val timer: Timer[IO]     = IO.timer(ec)
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

  val acquire: IO[Int]  = ???
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
    s.mapChunks(_ => Chunk.empty[INothing])

  def attempt[F[_]: RaiseThrowable, O](s: Stream[F, O]): Stream[F, Either[Throwable, O]] =
    s.map(Right(_)).handleErrorWith(t => Stream.emit(Left(t)))

  // *** scanChunksOpt vs Pull ***

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

  Stream(1, 2, 3, 4).through(tk(2)).toList

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

  def intersperse[F[_], O](separator: O): Pipe[F, O, O] = {
    def go(s: Stream[F, O]): Pull[F, O, Unit] =
      s.pull.echo1.flatMap {
        case None => Pull.done
        case Some(s) =>
          s.repeatPull {
              _.uncons.flatMap {
                case None => Pull.pure(None)
                case Some((hd, tl)) =>
                  val bldr = Vector.newBuilder[O]
                  bldr.sizeHint(hd.size * 2)
                  hd.foreach { o =>
                    bldr += separator
                    bldr += o
                  }
                  Pull.output(Chunk.vector(bldr.result)) >> Pull.pure(Some(tl))
              }
            }
            .pull
            .echo
      }
    in =>
      go(in).stream
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

  def rows[F[_]](h: CSVHandle)(implicit F: ConcurrentEffect[F],
                               cs: ContextShift[F]): Stream[F, Row] =
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
}
