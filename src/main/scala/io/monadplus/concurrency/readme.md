### Concurrency primitives
_For all F `implicit Concurrent[F]`_

__cats-effect:__

    - Ref[F [_], A]
    - MVar[F[_], A]
    - Deferred[F[_], A]
    - Semaphore[F[_]]

__fs2:__

    - Queue[F, A]
    - Topic[F, A]
    - Signal[F, A]
   
   