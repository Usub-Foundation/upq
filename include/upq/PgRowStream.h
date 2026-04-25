#ifndef PGROWSTREAM_H
#define PGROWSTREAM_H

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <cassert>

#include "PgConnection.h"
#include "PgReflect.h"
#include "PgTypes.h"
#include "uvent/Uvent.h"

namespace usub::pg {
    class PgPool;

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

        usub::uvent::task::Awaitable<std::optional<QueryResult::Row> > next() {
            if (!active_) co_return std::nullopt;

            if (buf_idx_ < buf_.size()) {
                co_return std::move(buf_[buf_idx_++]);
            }

            if (exhausted_) co_return std::nullopt;

            auto chunk = co_await conn_->cursor_fetch_chunk(cursor_name_, batch_size_);
            if (!chunk.ok) {
                err_ = PgOpError{chunk.code, chunk.error, chunk.err_detail};
                exhausted_ = true;
                co_return std::nullopt;
            }

            if (columns_.empty() && !chunk.columns.empty()) {
                columns_ = std::move(chunk.columns);
            }

            buf_ = std::move(chunk.rows);
            buf_idx_ = 0;

            if (buf_.empty()) {
                exhausted_ = true;
                co_return std::nullopt;
            }

            co_return std::move(buf_[buf_idx_++]);
        }

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

        usub::uvent::task::Awaitable<std::optional<T> > next() {
            auto row = co_await inner_.next();
            if (!row) co_return std::nullopt;

            QueryResult shim{};
            shim.ok = true;
            shim.rows_valid = true;
            shim.columns = inner_.columns();
            shim.rows.emplace_back(std::move(*row));
            try {
                if (!shim.columns.empty()) {
                    co_return usub::pg::map_single_reflect_named<T>(shim, 0);
                }
                co_return usub::pg::map_single_reflect_positional<T>(shim, 0);
            } catch (const std::exception &) {
                co_return usub::pg::map_single_reflect_positional<T>(shim, 0);
            }
        }

        usub::uvent::task::Awaitable<void> close() { return inner_.close(); }

        bool ok() const noexcept { return inner_.ok(); }
        const PgOpError &error() const noexcept { return inner_.error(); }
        bool active() const noexcept { return inner_.active(); }
        const std::vector<std::string> &columns() const noexcept { return inner_.columns(); }
        void set_batch_size(uint32_t n) noexcept { inner_.set_batch_size(n); }

    private:
        PgRowStream inner_;
    };
}

#endif
