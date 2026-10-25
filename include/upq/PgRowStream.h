#ifndef PGROWSTREAM_H
#define PGROWSTREAM_H

#include <cassert>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "PgConnection.h"
#include "PgReflect.h"
#include "PgTypes.h"
#include "uvent/Uvent.h"

namespace usub::pg {
    class PgPool;

    // Rows of a server-side cursor, fetched batch_size at a time. Owns the
    // cursor's transaction and pooled connection when opened by
    // PgPool::stream(); borrows the connection when opened inside a
    // PgTransaction. Call close() when done.
    class PgRowStream {
    public:
        using value_type = QueryResult::Row;

        PgRowStream() = default;

        PgRowStream(PgRowStream &&other) noexcept { move_from(std::move(other)); }

        PgRowStream &operator=(PgRowStream &&other) noexcept {
            if (this != &other) {
                assert(!active_ && "move-assign onto an active PgRowStream; call close() first");
                move_from(std::move(other));
            }
            return *this;
        }

        PgRowStream(const PgRowStream &) = delete;
        PgRowStream &operator=(const PgRowStream &) = delete;

        ~PgRowStream();

        usub::uvent::task::Awaitable<std::optional<QueryResult::Row> > next();

        usub::uvent::task::Awaitable<void> close();

        bool ok() const noexcept { return !err_.has_value(); }

        const PgOpError &error() const noexcept {
            static const PgOpError empty{};
            return err_ ? *err_ : empty;
        }

        bool active() const noexcept { return active_; }

        const std::vector<std::string> &columns() const noexcept { return columns_; }

        uint32_t batch_size() const noexcept { return batch_size_; }

        void set_batch_size(uint32_t n) noexcept { batch_size_ = n ? n : 1; }

    private:
        friend class PgPool;
        friend class PgTransaction;
        template<class T>
        friend class PgTypedRowStream;

        static PgRowStream owning(PgPool *pool, std::shared_ptr<PgConnectionLibpq> conn, std::string cursor,
                                  uint32_t batch_size);

        static PgRowStream borrowed(std::shared_ptr<PgConnectionLibpq> conn, std::string cursor,
                                    uint32_t batch_size);

        bool has_buffered() const noexcept { return active_ && buf_idx_ < buf_.size(); }

        QueryResult::Row &pop_buffered() noexcept { return buf_[buf_idx_++]; }

        // FETCHes the next batch into the buffer. False when the cursor is
        // exhausted or the fetch failed (error() says which).
        usub::uvent::task::Awaitable<bool> fetch();

        void move_from(PgRowStream &&o) noexcept {
            pool_ = o.pool_;
            conn_ = std::move(o.conn_);
            cursor_name_ = std::move(o.cursor_name_);
            batch_size_ = o.batch_size_;
            owns_tx_ = o.owns_tx_;
            active_ = o.active_;
            exhausted_ = o.exhausted_;
            buf_ = std::move(o.buf_);
            buf_idx_ = o.buf_idx_;
            err_ = std::move(o.err_);
            columns_ = std::move(o.columns_);

            o.pool_ = nullptr;
            o.active_ = false;
            o.exhausted_ = true;
            o.buf_idx_ = 0;
            o.owns_tx_ = false;
        }

        PgPool *pool_{nullptr};
        std::shared_ptr<PgConnectionLibpq> conn_;
        std::string cursor_name_;
        uint32_t batch_size_{1000};
        bool owns_tx_{false};
        bool active_{false};
        bool exhausted_{false};
        std::vector<QueryResult::Row> buf_;
        size_t buf_idx_{0};
        std::optional<PgOpError> err_;
        std::vector<std::string> columns_;
    };

    template <class T>
    class PgTypedRowStream {
    public:
        using value_type = T;

        PgTypedRowStream() = default;
        explicit PgTypedRowStream(PgRowStream inner) : inner_(std::move(inner)) {}

        PgTypedRowStream(PgTypedRowStream &&) noexcept = default;
        PgTypedRowStream &operator=(PgTypedRowStream &&) noexcept = default;
        PgTypedRowStream(const PgTypedRowStream &) = delete;
        PgTypedRowStream &operator=(const PgTypedRowStream &) = delete;

        // Next row as T: by column name when every field has a column,
        // positionally otherwise (std::runtime_error if that fails too).
        usub::uvent::task::Awaitable<std::optional<T> > next() {
            if (!inner_.has_buffered()) {
                const bool fetched = co_await inner_.fetch();
                if (!fetched) co_return std::nullopt;
            }
            co_return map(inner_.pop_buffered());
        }

        usub::uvent::task::Awaitable<void> close() { return inner_.close(); }

        bool ok() const noexcept { return inner_.ok(); }
        const PgOpError &error() const noexcept { return inner_.error(); }
        bool active() const noexcept { return inner_.active(); }
        const std::vector<std::string> &columns() const noexcept { return inner_.columns(); }
        void set_batch_size(uint32_t n) noexcept { inner_.set_batch_size(n); }

    private:
        T map(const QueryResult::Row &row) {
            const auto &cols = inner_.columns();
            if (!plan_ready_) {  // the columns are known once the first batch is in
                plan_ = detail::make_named_plan<T>(cols, false);
                plan_ready_ = true;
            }
            T dst{};
            if (plan_.usable && detail::decode_named(plan_, cols, row, dst, nullptr))
                return dst;
            dst = T{};
            std::string err;
            if (!detail::fill_from_row_positional_ex(row, cols.empty() ? nullptr : &cols, nullptr, dst, &err))
                throw std::runtime_error(err.empty() ? "decode failed" : err);
            return dst;
        }

        PgRowStream inner_;
        detail::NamedPlan<T> plan_{};
        bool plan_ready_{false};
    };
}

#endif
