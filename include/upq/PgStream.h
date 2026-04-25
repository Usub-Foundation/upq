#ifndef PGSTREAM_H
#define PGSTREAM_H

#include <cstddef>
#include <cstdint>
#include <concepts>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "PgRowStream.h"
#include "uvent/Uvent.h"

namespace usub::pg::stream {

    template <class S>
    concept PgStreamLike = requires(S &s) {
        typename S::value_type;
        { s.next() } -> std::same_as<
            usub::uvent::task::Awaitable<std::optional<typename S::value_type> > >;
    };

    template <class Up, class F>
    class TransformStream {
    public:
        using value_type = std::invoke_result_t<F &, typename Up::value_type>;

        TransformStream(Up up, F f) : up_(std::move(up)), f_(std::move(f)) {}
        TransformStream(TransformStream &&) noexcept = default;
        TransformStream &operator=(TransformStream &&) noexcept = default;
        TransformStream(const TransformStream &) = delete;
        TransformStream &operator=(const TransformStream &) = delete;

        usub::uvent::task::Awaitable<std::optional<value_type> > next() {
            auto v = co_await up_.next();
            if (!v) co_return std::nullopt;
            co_return f_(std::move(*v));
        }

        usub::uvent::task::Awaitable<void> close() {
            if constexpr (requires { up_.close(); }) co_await up_.close();
            co_return;
        }

    private:
        Up up_;
        F f_;
    };

    template <class Up, class F>
    class FilterStream {
    public:
        using value_type = typename Up::value_type;

        FilterStream(Up up, F f) : up_(std::move(up)), f_(std::move(f)) {}
        FilterStream(FilterStream &&) noexcept = default;
        FilterStream &operator=(FilterStream &&) noexcept = default;
        FilterStream(const FilterStream &) = delete;
        FilterStream &operator=(const FilterStream &) = delete;

        usub::uvent::task::Awaitable<std::optional<value_type> > next() {
            for (;;) {
                auto v = co_await up_.next();
                if (!v) co_return std::nullopt;
                if (f_(*v)) co_return std::move(*v);
            }
        }

        usub::uvent::task::Awaitable<void> close() {
            if constexpr (requires { up_.close(); }) co_await up_.close();
            co_return;
        }

    private:
        Up up_;
        F f_;
    };

    template <class Up>
    class TakeStream {
    public:
        using value_type = typename Up::value_type;

        TakeStream(Up up, size_t n) : up_(std::move(up)), rem_(n) {}
        TakeStream(TakeStream &&) noexcept = default;
        TakeStream &operator=(TakeStream &&) noexcept = default;
        TakeStream(const TakeStream &) = delete;
        TakeStream &operator=(const TakeStream &) = delete;

        usub::uvent::task::Awaitable<std::optional<value_type> > next() {
            if (rem_ == 0) co_return std::nullopt;
            auto v = co_await up_.next();
            if (!v) co_return std::nullopt;
            --rem_;
            co_return std::move(*v);
        }

        usub::uvent::task::Awaitable<void> close() {
            if constexpr (requires { up_.close(); }) co_await up_.close();
            co_return;
        }

    private:
        Up up_;
        size_t rem_;
    };

    template <class Up>
    class SkipStream {
    public:
        using value_type = typename Up::value_type;

        SkipStream(Up up, size_t n) : up_(std::move(up)), rem_(n) {}
        SkipStream(SkipStream &&) noexcept = default;
        SkipStream &operator=(SkipStream &&) noexcept = default;
        SkipStream(const SkipStream &) = delete;
        SkipStream &operator=(const SkipStream &) = delete;

        usub::uvent::task::Awaitable<std::optional<value_type> > next() {
            while (rem_ > 0) {
                auto v = co_await up_.next();
                if (!v) co_return std::nullopt;
                --rem_;
            }
            auto v = co_await up_.next();
            if (!v) co_return std::nullopt;
            co_return std::move(*v);
        }

        usub::uvent::task::Awaitable<void> close() {
            if constexpr (requires { up_.close(); }) co_await up_.close();
            co_return;
        }

    private:
        Up up_;
        size_t rem_;
    };

    template <class F>
    struct TransformAdaptor { F f; };

    template <class F>
    struct FilterAdaptor { F f; };

    struct TakeAdaptor { size_t n; };
    struct SkipAdaptor { size_t n; };

    template <class T>
    struct AsAdaptor {};

    template <class F>
    inline auto transform(F f) { return TransformAdaptor<F>{std::move(f)}; }

    template <class F>
    inline auto filter(F f) { return FilterAdaptor<F>{std::move(f)}; }

    inline auto take(size_t n) { return TakeAdaptor{n}; }
    inline auto skip(size_t n) { return SkipAdaptor{n}; }

    template <class T>
    inline constexpr AsAdaptor<T> as{};

    template <PgStreamLike S, class F>
    auto operator|(S s, TransformAdaptor<F> a) {
        return TransformStream<S, F>{std::move(s), std::move(a.f)};
    }

    template <PgStreamLike S, class F>
    auto operator|(S s, FilterAdaptor<F> a) {
        return FilterStream<S, F>{std::move(s), std::move(a.f)};
    }

    template <PgStreamLike S>
    auto operator|(S s, TakeAdaptor a) {
        return TakeStream<S>{std::move(s), a.n};
    }

    template <PgStreamLike S>
    auto operator|(S s, SkipAdaptor a) {
        return SkipStream<S>{std::move(s), a.n};
    }

    template <class T>
    PgTypedRowStream<T> operator|(PgRowStream s, AsAdaptor<T>) {
        return PgTypedRowStream<T>{std::move(s)};
    }

    struct CountTerminal {};
    inline constexpr CountTerminal count{};

    struct CollectTerminal {};
    inline constexpr CollectTerminal collect{};

    template <class F>
    struct ForEachTerminal { F f; };

    template <class F>
    inline auto for_each(F f) { return ForEachTerminal<F>{std::move(f)}; }

    template <class Acc, class F>
    struct ReduceTerminal {
        Acc init;
        F f;
    };

    template <class Acc, class F>
    inline auto reduce(Acc init, F f) {
        return ReduceTerminal<Acc, F>{std::move(init), std::move(f)};
    }

    namespace detail {
        template <class S>
        usub::uvent::task::Awaitable<void> close_if_possible(S &s) {
            if constexpr (requires { s.close(); }) {
                co_await s.close();
            }
            co_return;
        }
    }

    template <PgStreamLike S>
    usub::uvent::task::Awaitable<uint64_t>
    operator|(S s, CountTerminal) {
        uint64_t n = 0;
        while (co_await s.next()) ++n;
        co_await detail::close_if_possible(s);
        co_return n;
    }

    template <PgStreamLike S>
    usub::uvent::task::Awaitable<std::vector<typename S::value_type> >
    operator|(S s, CollectTerminal) {
        std::vector<typename S::value_type> out;
        while (auto v = co_await s.next()) out.emplace_back(std::move(*v));
        co_await detail::close_if_possible(s);
        co_return out;
    }

    template <PgStreamLike S, class F>
    usub::uvent::task::Awaitable<uint64_t>
    operator|(S s, ForEachTerminal<F> term) {
        uint64_t n = 0;
        while (auto v = co_await s.next()) {
            using R = decltype(term.f(std::move(*v)));
            if constexpr (std::is_same_v<R, usub::uvent::task::Awaitable<void> >) {
                co_await term.f(std::move(*v));
            } else {
                term.f(std::move(*v));
            }
            ++n;
        }
        co_await detail::close_if_possible(s);
        co_return n;
    }

    template <PgStreamLike S, class Acc, class F>
    usub::uvent::task::Awaitable<Acc>
    operator|(S s, ReduceTerminal<Acc, F> term) {
        Acc acc = std::move(term.init);
        while (auto v = co_await s.next()) {
            acc = term.f(std::move(acc), std::move(*v));
        }
        co_await detail::close_if_possible(s);
        co_return acc;
    }
}

#endif
