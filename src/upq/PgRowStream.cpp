#include "upq/PgRowStream.h"
#include "upq/PgPool.h"

namespace usub::pg {
    PgRowStream::~PgRowStream() {
        if (active_ && conn_ && pool_) {
            pool_->mark_dead(conn_);
            conn_.reset();
        }
        active_ = false;
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
        {
            QueryResult r = co_await conn_->cursor_close_in_tx(cursor_name_);
            if (!r.ok && is_fatal_connection_error(r)) fatal = true;
            if (!r.ok && !err_) {
                err_ = PgOpError{r.code, r.error, r.err_detail};
            }
        }

        if (!fatal && owns_tx_) {
            QueryResult r = co_await conn_->exec_simple_query_nonblocking("COMMIT;");
            if (!r.ok && is_fatal_connection_error(r)) fatal = true;
            if (!r.ok && !err_) {
                err_ = PgOpError{r.code, r.error, r.err_detail};
            }
        }

        if (pool_) {
            if (fatal) {
                pool_->mark_dead(conn_);
            } else {
                co_await pool_->release_connection_async(conn_);
            }
        }
        conn_.reset();
        co_return;
    }
}
