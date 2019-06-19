package io.monadplus

import cats.implicits._
import cats.effect._
import cats.effect.concurrent._
import cats.temp.par._

object Memoize {
  def memoize[F[_], A](fa: F[A])(implicit F: Async[F]): F[F[A]] =
    Ref.of[F, Option[Deferred[F, Either[Throwable, A]]]](none).map { ref =>
      Deferred.uncancelable[F, Either[Throwable, A]].flatMap { d =>
        ref
          .modify {
            case s @ Some(other) =>
              s -> other.get
            case None =>
              Some(d) -> fa.attempt.flatMap(eta => d.complete(eta).as(eta))
          }
          .flatten
          .rethrow
      }
    }
}

object MemoizeTest extends IOApp {

  def memo[F[_]](implicit F: Async[F]): F[_] =
    Ref.of[F, Int](0).flatMap { ref =>
      val fa: F[Unit] = ref.update(_ + 1)
      Memoize.memoize(fa).flatMap { mem =>
        mem.replicateA(10)
      } >> ref.get.flatTap(c => F.delay(println(s"[Memoized] Count: $c")))
    }

  def notMemo[F[_]](implicit F: Async[F]): F[_] =
    Ref.of[F, Int](0).flatMap { ref =>
      val fa: F[Unit] = ref.update(_ + 1)
      fa.replicateA(10) >> ref.get.flatTap(c => F.delay(println(s"[Not memoized] Count: $c")))
    }

  def race[F[_]: NonEmptyPar](implicit F: Async[F]): F[_] =
    Ref.of[F, Int](0).flatMap { ref =>
      val fa: F[Unit] = ref.update(_ + 1)
      Memoize.memoize(fa).flatMap { mem =>
        (mem, mem).parMapN((_, _) => ()) >> ref.get
          .flatTap(c => F.delay(println(s"[Race] Count: $c")))
      }
    }

  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- memo[IO]
      _ <- notMemo[IO]
      _ <- race[IO]
    } yield ExitCode.Success
}
