#include "upq/PgRowStream.h"
#include "upq/PgPool.h"

namespace usub::pg {
    PgRowStream PgRowStream::owning(PgPool *pool, std::shared_ptr<PgConnectionLibpq> conn, std::string cursor,
                                    uint32_t batch_size) {
        PgRowStream s = borrowed(std::move(conn), std::move(cursor), batch_size);
        s.pool_ = pool;
        s.owns_tx_ = true;
        return s;
    }

    PgRowStream PgRowStream::borrowed(std::shared_ptr<PgConnectionLibpq> conn, std::string cursor,
                                      uint32_t batch_size) {
        PgRowStream s;
        s.conn_ = std::move(conn);
        s.cursor_name_ = std::move(cursor);
        s.batch_size_ = batch_size ? batch_size : 1;
        s.active_ = true;
        s.exhausted_ = false;
        return s;
    }

    PgRowStream::~PgRowStream() {
        if (active_ && conn_ && pool_) {
            pool_->mark_dead(conn_);
            conn_.reset();
        }
        active_ = false;
    }

    usub::uvent::task::Awaitable<bool> PgRowStream::fetch() {
        if (!active_ || exhausted_) co_return false;

        auto chunk = co_await conn_->cursor_fetch_chunk(cursor_name_, batch_size_);
        if (!chunk.ok) {
            err_ = PgOpError{chunk.code, chunk.error, chunk.err_detail};
            exhausted_ = true;
            co_return false;
        }
        if (columns_.empty() && !chunk.columns.empty())
            columns_ = std::move(chunk.columns);

        buf_ = std::move(chunk.rows);
        buf_idx_ = 0;
        if (buf_.empty()) {
            exhausted_ = true;
            co_return false;
        }
        co_return true;
    }

    usub::uvent::task::Awaitable<std::optional<QueryResult::Row> > PgRowStream::next() {
        if (!has_buffered()) {
            const bool fetched = co_await fetch();
            if (!fetched) co_return std::nullopt;
        }
        co_return std::move(pop_buffered());
    }

    usub::uvent::task::Awaitable<void> PgRowStream::close() {
        if (!active_) co_return;
        active_ = false;

        if (!conn_ || !conn_->connected()) {
            if (pool_ && conn_) {
                pool_->mark_dead(conn_);
                conn_.reset();
            }
            co_return;
        }

        bool fatal = false;
        auto note = [&](const QueryResult &r) {
            if (r.ok) return;
            if (is_fatal_connection_error(r)) fatal = true;
            if (!err_) err_ = detail::to_op_error(r);
        };

        const QueryResult closed = co_await conn_->cursor_close_in_tx(cursor_name_);
        note(closed);
        if (!fatal && owns_tx_) {
            const QueryResult committed = co_await conn_->exec_simple_query_nonblocking("COMMIT;");
            note(committed);
        }

        if (pool_) {
            if (fatal) pool_->mark_dead(conn_);
            else co_await pool_->release_connection_async(conn_);
        }
        conn_.reset();
    }
}
