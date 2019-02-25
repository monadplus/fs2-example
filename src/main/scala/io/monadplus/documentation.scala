package io.monadplus

import cats.implicits._
import cats.effect._
import fs2._

import scala.annotation.tailrec

object Documentation extends App {
  // Stream[Pure, Int]
  val empty = Stream.empty
  Stream.emit(1)
  Stream(1, 2, 3)
  Stream.emits(List(1, 2, 3))
  // More operations
  (Stream(1, 2, 3) ++ Stream(4, 5)).toList // ++ takes constant time
  Stream(1, 2, 3).map(_ + 1).toList
  Stream(1, 2, 3).filter(_ % 2 != 0).toList
  Stream(1, 2, 3).fold(0)(_ + _).toList // List(6)
  Stream(none, 2.some, 3.some).collect { case Some(i) => i }.toList
  Stream.range(0, 5).intersperse(42).toList
  Stream(1, 2, 3).flatMap(i => Stream(i, i)).toList // List(1,1,2,2,3,3)
  Stream(1, 2, 3).repeat.take(9).toList             // List(1,2,3,1,2,3,1,2,3)

  // Effectful Streams
  val s1err = Stream.raiseError[IO](new Exception("BOOM !"))
  s1err.attempt
  s1err.handleErrorWith(e => Stream.emit(e.getMessage)).compile.toList.unsafeRunSync()
  val eff = Stream.eval(IO { println("BEING RUN!!"); 1 + 1 })
  Stream.attemptEval(IO.raiseError(new Exception("BOOM !")))
  eff.compile.toList.unsafeRunSync()
  eff.compile.drain //F[Unit]
  eff.compile.fold(0)(_ + _).unsafeRunSync()

  // Chunks: a strict, finite sequence of values that supports efficient indexed based lookup of elements.
  val s1c: Stream[Pure, Double] = Stream.chunk(Chunk.doubles(Array(1.0, 2.0, 3.0)))
  s1c.mapChunks { ds =>
    /* do things*/
    ds.toDoubles
  }

  // Resource acquisition
  val acquire: IO[Int]  = ???
  val release: IO[Unit] = ???
  Stream
    .bracket(acquire)(_ => release)
    .compile
    .drain
    .unsafeRunSync()

  // Exercises
  def repeat[F[_], O](s: Stream[F, O]): Stream[F, O] =
    s ++ repeat(s) // trick: ++ is called by name

  def drain[F[_], O](s: Stream[F, O]): Stream[F, INothing] =
    s.mapChunks(_ => Chunk.empty[INothing])

  def attempt[F[_]: RaiseThrowable, O](s: Stream[F, O]): Stream[F, Either[Throwable, O]] =
    s.map(Right(_)).handleErrorWith(t => Stream.emit(Left(t)))

  // Pipe[F[_], -I, +O] = Stream[F, I] => Stream[F, O]
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

  // Pull (docs)
  def tk[F[_], O](n: Long): Pipe[F, O, O] = {
    def go(s: Stream[F, O], n: Long): Pull[F, O, Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) =>
          hd.size match {
            case m if m <= n => Pull.output(hd) >> go(tl, n - m)
            case m           => Pull.output(hd.take(n.toInt)) >> Pull.done
          }
        case None => Pull.done
      }
    in =>
      go(in, n).stream
  }

  // Exercises:

  // takeWhile (in this example R can be Unit and simplifies things)
  def takeWhile[F[_], O](p: O => Boolean): Pipe[F, O, O] = {
    def go(s: Stream[F, O]): Pull[F, O, Option[Stream[F, O]]] =
      s.pull.uncons.flatMap {
        case None => Pull.pure(none)
        case Some((hd, tl)) =>
          hd.indexWhere(o => !p(o)) match {
            case None => Pull.output(hd) >> go(tl)
            case Some(idx) =>
              val (pfx, sfx) = hd.splitAt(idx)
              Pull.output(pfx) >> Pull.pure(tl.cons(sfx).some)
          }
      }
    in =>
      go(in).stream
  }

  // intersperse: this was done copying the fs2 impl.
  def intersperse[F[_], O](separator: O): Pipe[F, O, O] = {
    def go(s: Stream[F, O]): Pull[F, O, Unit] =
      s.pull.echo1.flatMap {
        case None => Pull.pure(None)
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

  // scan
  // TODO
  // hint: scan => output1(z) >> scan(...)

}
