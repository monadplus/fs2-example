package io.monadplus

import cats.implicits._
import cats.effect._

object Memoize {
  // Ref + Def, (ref.of Option + def Either throwable A
  def memoize[F[_], A](f: F[A])(implicit F: Async[F]): F[F[A]] =
    ???
}

object MemoizeTest extends IOApp {
  import Memoize._

  def run(args: List[String]): IO[ExitCode] =
    IO.pure(ExitCode.Success)
}

