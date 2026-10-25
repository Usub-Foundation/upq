#ifndef UPQ_UVENT_COMPAT_H
#define UPQ_UVENT_COMPAT_H

#include <chrono>

#include "uvent/Uvent.h"

// upq builds against every uvent its users still pin. Releases before the
// structured-concurrency merge (e.g. dc51cf5) have no cooperative
// cancellation, and their sleep_for / AsyncSemaphore::acquire return void;
// sync/CancelState.h arrived in the same change, so its presence selects the
// API.
#if __has_include("uvent/sync/CancelState.h")
#define UPQ_UVENT_HAS_CANCELLATION 1
#else
#define UPQ_UVENT_HAS_CANCELLATION 0
#endif

namespace usub::pg::detail {
    inline bool cancel_requested() noexcept {
#if UPQ_UVENT_HAS_CANCELLATION
        return uvent::system::this_coroutine::cancel_requested();
#else
        return false;
#endif
    }

    // co_await sleep_for(d) yields false when the sleep was cut short by
    // cancellation (never, on a uvent without cancellation).
#if UPQ_UVENT_HAS_CANCELLATION
    template <class Rep, class Period>
    auto sleep_for(std::chrono::duration<Rep, Period> d) {
        return uvent::system::this_coroutine::sleep_for(d);
    }
#else
    template <class Rep, class Period>
    uvent::task::Awaitable<bool> sleep_for(std::chrono::duration<Rep, Period> d) {
        co_await uvent::system::this_coroutine::sleep_for(d);
        co_return true;
    }
#endif

    // co_await acquire(sem) yields false when canceled: the permit is not taken.
#if UPQ_UVENT_HAS_CANCELLATION
    template <class Semaphore>
    auto acquire(Semaphore &sem) {
        return sem.acquire();
    }
#else
    template <class Semaphore>
    uvent::task::Awaitable<bool> acquire(Semaphore &sem) {
        co_await sem.acquire();
        co_return true;
    }
#endif
} // namespace usub::pg::detail

#endif // UPQ_UVENT_COMPAT_H
