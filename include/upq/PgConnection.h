#ifndef PGCONNECTIONLIBPQ_H
#define PGCONNECTIONLIBPQ_H

#include <bit>
#include <atomic>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

#include <libpq-fe.h>

#include "PgJsonCodec.h"
#include "PgReflect.h"
#include "PgTypes.h"
#include "meta/PgConcepts.h"
#include "uvent/Uvent.h"

#ifndef UPQ_REFLECT_DEBUG
#define UPQ_REFLECT_DEBUG 1
#endif

#if UPQ_REFLECT_DEBUG
#include <cstdio>
#define UPQ_CONN_DBG(fmt, ...) \
    do { std::fprintf(stderr, "[UPQ/conn] " fmt "\n", ##__VA_ARGS__); } while (0)
#else
#define UPQ_CONN_DBG(fmt, ...) \
    do { } while (0)
#endif

namespace usub::pg {
    inline void fill_server_error_fields(PGresult *res, QueryResult &out) {
        if (!res) return;

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char *primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char *detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char *hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char *fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary) out.err_detail.message = primary;
        else if (!out.error.empty()) out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
        out.rows_valid = false;
    }

    static inline uint64_t extract_rows_affected(PGresult *res) {
        if (!res) return 0;
        if (const char *aff = PQcmdTuples(res); aff && *aff)
            return std::strtoull(aff, nullptr, 10);

        if (const char *tag = PQcmdStatus(res); tag && *tag) {
            const char *end = tag + std::strlen(tag);
            while (end > tag && std::isspace(static_cast<unsigned char>(*(end - 1)))) --end;
            const char *p = end;
            while (p > tag && std::isdigit(static_cast<unsigned char>(*(p - 1)))) --p;
            if (p < end) return std::strtoull(p, nullptr, 10);
        }
        return 0;
    }

    constexpr uint16_t to_be16(uint16_t v) noexcept {
        if constexpr (std::endian::native == std::endian::little) return std::byteswap(v);
        else return v;
    }

    constexpr uint32_t to_be32(uint32_t v) noexcept {
        if constexpr (std::endian::native == std::endian::little) return std::byteswap(v);
        else return v;
    }

    constexpr uint64_t to_be64(uint64_t v) noexcept {
        if constexpr (std::endian::native == std::endian::little) return std::byteswap(v);
        else return v;
    }

    inline uint32_t fp_to_be(float v) noexcept {
        static_assert(std::numeric_limits<float>::is_iec559);
        return to_be32(std::bit_cast<uint32_t>(v));
    }

    inline uint64_t fp_to_be(double v) noexcept {
        static_assert(std::numeric_limits<double>::is_iec559);
        return to_be64(std::bit_cast<uint64_t>(v));
    }

    struct ParamSlices {
        const char **values;
        int *lengths;
        int *formats;
        Oid *types;
        size_t *idx;

        std::vector<std::string> &temp_strings;
        std::vector<std::vector<char> > &temp_bytes;

        void set_null() {
            const auto i = (*idx)++;
            values[i] = nullptr;
            lengths[i] = 0;
            formats[i] = 0;
            types[i] = 0;
        }

        void set_text(std::string_view sv) {
            const auto i = (*idx)++;
            temp_strings.emplace_back(sv);
            values[i] = temp_strings.back().c_str();
            lengths[i] = static_cast<int>(sv.size());
            formats[i] = 0;
            types[i] = 0;
        }

        void set_text_typed(std::string_view sv, Oid oid) {
            const auto i = (*idx)++;
            temp_strings.emplace_back(sv);
            values[i] = temp_strings.back().c_str();
            lengths[i] = static_cast<int>(sv.size());
            formats[i] = 0;
            types[i] = oid;
        }

        void set_bin_raw(const void *data, size_t n, Oid oid) {
            const auto i = (*idx)++;
            temp_bytes.emplace_back();
            auto &buf = temp_bytes.back();
            buf.resize(n);
            std::memcpy(buf.data(), data, n);
            values[i] = buf.data();
            lengths[i] = static_cast<int>(n);
            formats[i] = 1;
            types[i] = oid;
        }

        template<class IntT>
        void set_bin_integral(IntT v, Oid oid) {
            if constexpr (sizeof(IntT) == 2) {
                uint16_t be = to_be16(static_cast<uint16_t>(v));
                set_bin_raw(&be, 2, oid);
            } else if constexpr (sizeof(IntT) == 4) {
                uint32_t be = to_be32(static_cast<uint32_t>(v));
                set_bin_raw(&be, 4, oid);
            } else {
                uint64_t be = to_be64(static_cast<uint64_t>(v));
                set_bin_raw(&be, 8, oid);
            }
        }

        void set_bin_f32(float v) {
            uint32_t be = fp_to_be(v);
            set_bin_raw(&be, 4, detail::FLOAT4OID);
        }

        void set_bin_f64(double v) {
            uint64_t be = fp_to_be(v);
            set_bin_raw(&be, 8, detail::FLOAT8OID);
        }

        void set_bin_bool(bool v) {
            unsigned char b = v ? 1u : 0u;
            set_bin_raw(&b, 1, detail::BOOLOID);
        }
    };

    namespace detail {
        inline void encode_one(ParamSlices &ps, bool v);

        template<Integral T>
        inline void encode_one(ParamSlices &ps, T v);

        inline void encode_one(ParamSlices &ps, float v);

        inline void encode_one(ParamSlices &ps, double v);

        inline void encode_one(ParamSlices &ps, std::string_view v);

        inline void encode_one(ParamSlices &ps, const std::string &v);

        template<size_t N>
        inline void encode_one(ParamSlices &ps, const char (&lit)[N]);

        inline void encode_one(ParamSlices &ps, const char *v);

        template<EnumType E>
        inline void encode_one(ParamSlices &ps, E v);

        template<class T, bool Strict, bool Jsonb>
        inline void encode_one(ParamSlices &ps, const ::usub::pg::PgJsonParam<T, Strict, Jsonb> &p);

        template<class T, bool Strict>
        inline void encode_one(ParamSlices &ps, const ::usub::pg::PgJson<T, Strict> &v);

        template<ArrayLike C>
        inline void encode_one(ParamSlices &ps, const C &cont);

        template<class T, class A>
        inline void encode_one(ParamSlices &ps, const std::vector<T, A> &cont);

        template<class T, size_t N>
        inline void encode_one(ParamSlices &ps, const T (&arr)[N]);

        template<class T, size_t N>
        inline void encode_one(ParamSlices &ps, T (&arr)[N]);

        template<class T>
        inline void encode_one(ParamSlices &ps, std::initializer_list<T> il);

        template<class Tup>
            requires (::usub::pg::detail::is_tuple_like_v<std::decay_t<Tup> >
                      && !ArrayLike<std::decay_t<Tup> >
                      && !CArrayLike<std::decay_t<Tup> >)
        inline void encode_one(ParamSlices &ps, const Tup &tup);

        template<class T>
            requires (::usub::pg::detail::ReflectAggregate<std::decay_t<T> >
                      && !::usub::pg::detail::is_tuple_like_v<std::decay_t<T> >
                      && !ArrayLike<std::decay_t<T> >
                      && !CArrayLike<std::decay_t<T> >)
        inline void encode_one(ParamSlices &ps, const T &obj);

        template<class T>
            requires (!Integral<std::decay_t<T> >
                      && !Floating<std::decay_t<T> >
                      && !StringLike<std::decay_t<T> >
                      && !CharPtr<std::decay_t<T> >
                      && !Optional<std::decay_t<T> >
                      && !ArrayLike<std::decay_t<T> >
                      && !CArrayLike<std::decay_t<T> >
                      && !AssociativeLike<std::decay_t<T> >
                      && !InitList<std::decay_t<T> >
                      && !EnumType<std::decay_t<T> >
                      && !ReflectAggregate<std::decay_t<T> >
                      && !is_tuple_like_v<std::decay_t<T> >
                      && !std::is_same_v<std::decay_t<T>, bool>)
        inline void encode_one(ParamSlices &ps, T &&v);

        template<class Opt>
            requires Optional<std::decay_t<Opt> >
        inline void encode_one(ParamSlices &ps, Opt &&ov);

        inline void encode_one(ParamSlices &ps, bool v) { ps.set_bin_bool(v); }

        template<Integral T>
        inline void encode_one(ParamSlices &ps, T v) {
            if constexpr (sizeof(T) <= 2)
                ps.set_bin_integral<int16_t>(static_cast<int16_t>(v), INT2OID);
            else if constexpr (sizeof(T) == 4)
                ps.set_bin_integral<int32_t>(static_cast<int32_t>(v), INT4OID);
            else
                ps.set_bin_integral<int64_t>(static_cast<int64_t>(v), INT8OID);
        }

        inline void encode_one(ParamSlices &ps, float v) { ps.set_bin_f32(v); }
        inline void encode_one(ParamSlices &ps, double v) { ps.set_bin_f64(v); }

        inline void encode_one(ParamSlices &ps, std::string_view v) { ps.set_text(v); }
        inline void encode_one(ParamSlices &ps, const std::string &v) { ps.set_text(v); }

        template<size_t N>
        inline void encode_one(ParamSlices &ps, const char (&lit)[N]) {
            ps.set_text(std::string_view(lit, N ? (N - 1) : 0));
        }

        inline void encode_one(ParamSlices &ps, const char *v) {
            if (v) ps.set_text(std::string_view(v, std::char_traits<char>::length(v)));
            else ps.set_null();
        }

        template<EnumType E>
        inline void encode_one(ParamSlices &ps, E v) {
            std::string tok;
            if (enum_to_token_impl<E>(v, tok)) {
                ps.set_text(tok);
                return;
            }
            using U = std::underlying_type_t<E>;
            if constexpr (sizeof(U) <= 2)
                ps.set_bin_integral<uint16_t>(static_cast<uint16_t>(static_cast<U>(v)), INT2OID);
            else if constexpr (sizeof(U) == 4)
                ps.set_bin_integral<uint32_t>(static_cast<uint32_t>(static_cast<U>(v)), INT4OID);
            else
                ps.set_bin_integral<uint64_t>(static_cast<uint64_t>(static_cast<U>(v)), INT8OID);
        }

        template<class T, bool Strict, bool Jsonb>
        inline void encode_one(ParamSlices &ps, const ::usub::pg::PgJsonParam<T, Strict, Jsonb> &p) {
            (void) Strict;
            if (!p.ptr) {
                ps.set_null();
                return;
            }

            std::string s = ::usub::pg::json::dump(*p.ptr);
            ps.set_text_typed(s, Jsonb ? ::usub::pg::detail::JSONBOID : ::usub::pg::detail::JSONOID);
        }

        template<class T, bool Strict>
        inline void encode_one(ParamSlices &ps, const ::usub::pg::PgJson<T, Strict> &v) {
            encode_one(ps, ::usub::pg::pg_jsonb<T, Strict>(v.value));
        }

        inline std::string escape_pg_array_elem(std::string_view in) {
            std::string out;
            out.reserve(in.size() + 4);
            for (char ch: in) {
                if (ch == '"' || ch == '\\') out.push_back('\\');
                out.push_back(ch);
            }
            return out;
        }

        template<class T>
        inline std::string to_text_number(T v) {
            char buf[128];
            auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), v);
            if (ec != std::errc{}) return {};
            return std::string(buf, p);
        }

        inline std::string to_text_number(float v) {
            std::ostringstream oss;
            oss << v;
            return oss.str();
        }

        inline std::string to_text_number(double v) {
            std::ostringstream oss;
            oss << v;
            return oss.str();
        }

        template<class Elem>
        inline void append_pg_array_elem(std::string &s, const Elem &v) {
            if constexpr (EnumType<Elem>) {
                std::string tok;
                if (!enum_to_token_impl<Elem>(v, tok)) {
                    using U = std::underlying_type_t<Elem>;
                    tok = std::to_string(static_cast<U>(v));
                }
                s.push_back('"');
                s.append(escape_pg_array_elem(tok));
                s.push_back('"');
            } else if constexpr (std::is_same_v<std::decay_t<Elem>, bool>) {
                s.append(v ? "true" : "false");
            } else if constexpr (StringLike<Elem> || std::is_same_v<std::decay_t<Elem>, std::string_view>) {
                std::string_view sv(v);
                s.push_back('"');
                s.append(escape_pg_array_elem(sv));
                s.push_back('"');
            } else if constexpr (CharPtr<Elem>) {
                const char *p = v;
                std::string_view sv = p ? std::string_view(p) : std::string_view{};
                s.push_back('"');
                s.append(escape_pg_array_elem(sv));
                s.push_back('"');
            } else if constexpr (Integral<Elem>) {
                s.append(to_text_number(v));
            } else if constexpr (Floating<Elem>) {
                s.append(to_text_number(static_cast<double>(v)));
            } else {
                std::ostringstream oss;
                oss << v;
                s.push_back('"');
                s.append(escape_pg_array_elem(oss.str()));
                s.push_back('"');
            }
        }

        template<class Range>
        inline std::string build_pg_text_array_literal_any(const Range &cont) {
            std::string s;
            s.push_back('{');
            bool first = true;

            for (auto &&e0: cont) {
                if (!first) s.push_back(',');
                first = false;

                using E0 = std::decay_t<decltype(e0)>;
                if constexpr (Optional<E0>) {
                    if (!e0) {
                        s.append("NULL");
                        continue;
                    }
                    using Elem = unopt_t<E0>;
                    append_pg_array_elem<Elem>(s, *e0);
                } else {
                    using Elem = std::decay_t<decltype(e0)>;
                    append_pg_array_elem<Elem>(s, e0);
                }
            }

            s.push_back('}');
            return s;
        }

        template<class Elem>
        consteval Oid array_oid_for_elem() {
            return pick_array_oid<Elem>();
        }

        template<ArrayLike C>
        inline void encode_one(ParamSlices &ps, const C &cont) {
            using VT = typename std::decay_t<C>::value_type;
            using E = unopt_t<VT>;
            const std::string s = build_pg_text_array_literal_any(cont);
            ps.set_text_typed(s, array_oid_for_elem<E>());
        }

        template<class T, class A>
        inline void encode_one(ParamSlices &ps, const std::vector<T, A> &cont) {
            using E = unopt_t<T>;
            const std::string s = build_pg_text_array_literal_any(cont);
            ps.set_text_typed(s, array_oid_for_elem<E>());
        }

        template<class T, size_t N>
        inline void encode_one(ParamSlices &ps, const T (&arr)[N]) {
            struct PtrRange {
                const T *b;
                const T *e;
                const T * begin() const { return b; }
                const T * end() const { return e; }
            } r{arr, arr + N};

            using E = unopt_t<T>;
            const std::string s = build_pg_text_array_literal_any(r);
            ps.set_text_typed(s, array_oid_for_elem<E>());
        }

        template<class T, size_t N>
        inline void encode_one(ParamSlices &ps, T (&arr)[N]) {
            encode_one(ps, const_cast<const T(&)[N]>(arr));
        }

        template<class T>
        inline void encode_one(ParamSlices &ps, std::initializer_list<T> il) {
            using E = unopt_t<T>;
            const std::string s = build_pg_text_array_literal_any(il);
            ps.set_text_typed(s, array_oid_for_elem<E>());
        }

        template<class Tup>
            requires (::usub::pg::detail::is_tuple_like_v<std::decay_t<Tup> >
                      && !ArrayLike<std::decay_t<Tup> >
                      && !CArrayLike<std::decay_t<Tup> >)
        inline void encode_one(ParamSlices &ps, const Tup &tup) {
            using DT = std::decay_t<Tup>;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (encode_one(ps, std::get<I>(tup)), ...);
            }(std::make_index_sequence<std::tuple_size_v<DT> >{});
        }

        template<class T>
            requires (::usub::pg::detail::ReflectAggregate<std::decay_t<T> >
                      && !::usub::pg::detail::is_tuple_like_v<std::decay_t<T> >
                      && !ArrayLike<std::decay_t<T> >
                      && !CArrayLike<std::decay_t<T> >)
        inline void encode_one(ParamSlices &ps, const T &obj) {
            using V = std::decay_t<T>;
            auto &nonconst = const_cast<V &>(obj);
            auto tiev = ureflect::to_tie(nonconst);
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (encode_one(ps, ureflect::get<I>(tiev)), ...);
            }(std::make_index_sequence<ureflect::count_members<V> >{});
        }

        template<class T>
            requires (!Integral<std::decay_t<T> >
                      && !Floating<std::decay_t<T> >
                      && !StringLike<std::decay_t<T> >
                      && !CharPtr<std::decay_t<T> >
                      && !Optional<std::decay_t<T> >
                      && !ArrayLike<std::decay_t<T> >
                      && !CArrayLike<std::decay_t<T> >
                      && !AssociativeLike<std::decay_t<T> >
                      && !InitList<std::decay_t<T> >
                      && !EnumType<std::decay_t<T> >
                      && !ReflectAggregate<std::decay_t<T> >
                      && !is_tuple_like_v<std::decay_t<T> >
                      && !std::is_same_v<std::decay_t<T>, bool>)
        inline void encode_one(ParamSlices &ps, T &&v) {
            if constexpr (std::is_convertible_v<T, std::string_view>)
                ps.set_text(std::string_view(v));
            else if constexpr (std::is_constructible_v<std::string, T>)
                ps.set_text(std::string(std::forward<T>(v)));
            else if constexpr (Streamable<T>) {
                std::ostringstream oss;
                oss << v;
                ps.set_text(oss.str());
            } else {
                std::ostringstream oss;
                oss << "<param:" << typeid(std::decay_t<T>).name() << ">";
                ps.set_text(oss.str());
            }
        }

        template<class Opt>
            requires Optional<std::decay_t<Opt> >
        inline void encode_one(ParamSlices &ps, Opt &&ov) {
            if (!ov) {
                ps.set_null();
                return;
            }
            encode_one(ps, *ov);
        }

        template<class T>
        struct param_arity_impl {
            static constexpr size_t value = 1;
        };

        template<class Opt>
            requires Optional<std::decay_t<Opt> >
        struct param_arity_impl<Opt> {
            using Inner = typename std::decay_t<Opt>::value_type;
            static constexpr size_t value = param_arity_impl<Inner>::value;
        };

        template<class C>
            requires ArrayLike<std::decay_t<C> >
        struct param_arity_impl<C> {
            static constexpr size_t value = 1;
        };

        template<class T, size_t N>
        struct param_arity_impl<T[N]> {
            static constexpr size_t value = 1;
        };

        template<class T>
        struct param_arity_impl<std::initializer_list<T> > {
            static constexpr size_t value = 1;
        };

        template<class Tup>
            requires (::usub::pg::detail::is_tuple_like_v<std::decay_t<Tup> >
                      && !ArrayLike<std::decay_t<Tup> >
                      && !CArrayLike<std::decay_t<Tup> >)
        struct param_arity_impl<Tup> {
            static constexpr size_t value = std::tuple_size_v<std::decay_t<Tup> >;
        };

        template<class T>
            requires (::usub::pg::detail::ReflectAggregate<std::decay_t<T> >
                      && !::usub::pg::detail::is_tuple_like_v<std::decay_t<T> >
                      && !ArrayLike<std::decay_t<T> >
                      && !CArrayLike<std::decay_t<T> >)
        struct param_arity_impl<T> {
            static constexpr size_t value = ureflect::count_members<std::decay_t<T> >;
        };

        template<class T>
        struct param_arity : param_arity_impl<std::decay_t<T> > {
        };

        template<class... Args>
        constexpr size_t count_total_params() {
            return (param_arity<Args>::value + ... + 0);
        }

        constexpr size_t count_pg_params(std::string_view sql) {
            size_t max_param = 0;
            for (size_t i = 0; i + 1 < sql.size(); ++i) {
                if (sql[i] == '$') {
                    size_t num = 0;
                    size_t j = i + 1;
                    while (j < sql.size() && sql[j] >= '0' && sql[j] <= '9') {
                        num = num * 10 + static_cast<size_t>(sql[j] - '0');
                        ++j;
                    }
                    if (j > i + 1 && num > max_param)
                        max_param = num;
                }
            }
            return max_param;
        }
    } // namespace detail

    enum class SSLMode {
        disable,
        allow,
        prefer,
        require,
        verify_ca,
        verify_full
    };

    static inline std::string to_string(SSLMode m) {
        switch (m) {
            case SSLMode::disable: return "disable";
            case SSLMode::allow: return "allow";
            case SSLMode::prefer: return "prefer";
            case SSLMode::require: return "require";
            case SSLMode::verify_ca: return "verify-ca";
            case SSLMode::verify_full: return "verify-full";
        }
        return "prefer";
    }

    struct SSLConfig {
        SSLMode mode{SSLMode::prefer};

        // For verify-ca / verify-full
        std::optional<std::string> root_cert; // CA bundle (path or PEM)
        std::optional<std::string> crl; // optional

        // For mTLS (client auth)
        std::optional<std::string> client_cert; // path or PEM
        std::optional<std::string> client_key; // path or PEM

        // For verify-full when connecting by IP but validating DNS name
        std::optional<std::string> server_hostname;
    };

    struct TCPKeepaliveConfig {
        bool enabled{false};

        int keepalives{1}; // 0/1
        int idle{30}; // idle before probes
        int interval{10}; // between probes
        int count{3}; // probes before drop
    };

    class PgConnectionLibpq {
    public:
        template<class HandlerT>
            requires PgNotifyHandler<HandlerT>
        friend class PgNotificationListener;

        friend class PgNotificationMultiplexer;
        friend class PgPool;

        PgConnectionLibpq();

        ~PgConnectionLibpq();

        usub::uvent::task::Awaitable<std::optional<std::string> >
        connect_async(const std::string &conninfo);

        usub::uvent::task::Awaitable<std::optional<std::string> >
        connect_async(const std::string &conninfo, std::chrono::milliseconds timeout);

        [[nodiscard]] bool connected() const noexcept;

        usub::uvent::task::Awaitable<QueryResult>
        exec_simple_query_nonblocking(const std::string &sql);

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        exec_param_query_nonblocking(const std::string &sql, Args &&... args);

        template<class T>
        usub::uvent::task::Awaitable<std::vector<T> >
        exec_simple_query_nonblocking(const std::string &sql) {
            using Fn = usub::uvent::task::Awaitable<usub::pg::QueryResult>
                    (PgConnectionLibpq::*)(const std::string &);
            Fn base = &PgConnectionLibpq::exec_simple_query_nonblocking;

            usub::pg::QueryResult qr = co_await (this->*base)(sql);
            try {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return vec;
            } catch (const std::exception &e) {
                UPQ_CONN_DBG("named-map FAIL: %s — fallback to positional", e.what());
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return vec;
            }
        }

        template<class T>
        usub::uvent::task::Awaitable<std::optional<T> >
        exec_simple_query_one_nonblocking(const std::string &sql) {
            using Fn = usub::uvent::task::Awaitable<usub::pg::QueryResult>
                    (PgConnectionLibpq::*)(const std::string &);
            Fn base = &PgConnectionLibpq::exec_simple_query_nonblocking;

            usub::pg::QueryResult qr = co_await (this->*base)(sql);
            if (qr.rows.empty()) co_return std::nullopt;
            try {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return v;
            } catch (const std::exception &e) {
                UPQ_CONN_DBG("named-one FAIL: %s — fallback to positional", e.what());
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return v;
            }
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::vector<T> >
        exec_param_query_nonblocking(const std::string &sql, Args &&... args) {
            usub::pg::QueryResult qr = co_await this->exec_param_query_nonblocking(
                sql, std::forward<Args>(args)...);

            if (!qr.ok) co_return std::vector<T>{};

            try {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return vec;
            } catch (const std::exception &e) {
                UPQ_CONN_DBG("param-named-map FAIL: %s — fallback to positional", e.what());
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return vec;
            }
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::optional<T> >
        exec_param_query_one_nonblocking(const std::string &sql, Args &&... args) {
            usub::pg::QueryResult qr = co_await this->exec_param_query_nonblocking(
                sql, std::forward<Args>(args)...);

            if (!qr.ok || qr.rows.empty()) co_return std::nullopt;

            try {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return v;
            } catch (const std::exception &e) {
                UPQ_CONN_DBG("param-named-one FAIL: %s — fallback to positional", e.what());
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return v;
            }
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        exec_param_query_nonblocking(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            const size_t expected = detail::count_pg_params(sv);
            assert(expected == actual &&
                "pg: $N placeholder count does not match argument count");
            co_return co_await exec_param_query_nonblocking(
                std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        exec_param_query_nonblocking(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            const size_t expected = detail::count_pg_params(sv);
            assert(expected == actual &&
                "pg: $N placeholder count does not match argument count");
            co_return co_await exec_param_query_nonblocking<T>(
                std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        exec_param_query_one_nonblocking(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            const size_t expected = detail::count_pg_params(sv);
            assert(expected == actual &&
                "pg: $N placeholder count does not match argument count");
            co_return co_await exec_param_query_one_nonblocking<T>(
                std::string(sv), std::forward<Args>(args)...);
        }

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_start(const std::string &sql);

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_send_chunk(const void *data, size_t len);

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_finish();

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_finish(const char *error_msg);

        usub::uvent::task::Awaitable<PgCopyResult> copy_out_start(const std::string &sql);

        usub::uvent::task::Awaitable<PgWireResult<std::vector<uint8_t> > > copy_out_read_chunk();

        template<class Sink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to(const std::string &sql, Sink &&sink);

        template<class LineSink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to_lines(const std::string &sql, LineSink &&line_sink);

        template<class Source>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from(const std::string &sql, Source &&source);

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from_buffer(const std::string &sql, const void *data, size_t len);

        usub::uvent::task::Awaitable<std::expected<std::string, PgOpError> >
        export_snapshot();

        usub::uvent::task::Awaitable<std::optional<PgOpError> >
        set_snapshot(const std::string &snapshot_id);

        std::string make_cursor_name();

        static bool is_safe_cursor_ident(const std::string &s);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare(const std::string &cursor_name, const std::string &sql);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare_in_tx(const std::string &cursor_name, const std::string &sql);

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare_in_tx_params(const std::string &cursor_name,
                                    const std::string &sql,
                                    Args &&... args) {
            if (!is_safe_cursor_ident(cursor_name)) {
                QueryResult err{};
                err.ok = false;
                err.code = PgErrorCode::ProtocolCorrupt;
                err.error = "invalid cursor name";
                err.rows_valid = false;
                co_return err;
            }
            const std::string stmt =
                    "DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql;
            co_return co_await exec_param_query_nonblocking(
                stmt, std::forward<Args>(args)...);
        }

        usub::uvent::task::Awaitable<PgCursorChunk>
        cursor_fetch_chunk(const std::string &cursor_name, uint32_t count);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_close(const std::string &cursor_name);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_close_in_tx(const std::string &cursor_name);

        PGconn *raw_conn() noexcept;

        bool is_idle();

        void close();

    private:
        usub::uvent::task::Awaitable<void> wait_readable();

        usub::uvent::task::Awaitable<void> wait_writable();

        usub::uvent::task::Awaitable<void> wait_readable_for_listener();

        usub::uvent::task::Awaitable<bool> flush_outgoing();

        usub::uvent::task::Awaitable<bool> pump_input();

        QueryResult drain_all_results();

        PgCopyResult drain_copy_end_result();

        PgCursorChunk drain_single_result_rows();

    private:
        PGconn *conn_{nullptr};
        bool connected_{false};

        std::unique_ptr<
            usub::uvent::net::Socket<
                usub::uvent::net::Proto::TCP,
                usub::uvent::net::Role::ACTIVE
            >
        > sock_;

        uint64_t cursor_seq_{0};

        mutable std::mutex close_mtx_;

    public:
        std::atomic<bool> dead_accounted_{false};
    };

    template<typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_param_query_nonblocking(const std::string &sql, Args &&... args) {
        QueryResult out{};
        out.ok = false;
        out.rows_affected = 0;

        if (!connected()) {
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        constexpr size_t M = detail::count_total_params<Args...>();

        std::vector<const char *> values(M ? M : 1, nullptr);
        std::vector<int> lengths(M ? M : 1, 0);
        std::vector<int> formats(M ? M : 1, 0);
        std::vector<Oid> types(M ? M : 1, 0);

        std::vector<std::string> temp_strings;
        temp_strings.reserve(M ? M : 1);

        std::vector<std::vector<char> > temp_bytes;
        temp_bytes.reserve(M ? M : 1);

        size_t idx = 0;
        ParamSlices ps{
            values.data(),
            lengths.data(),
            formats.data(),
            types.data(),
            &idx,
            temp_strings,
            temp_bytes
        };

        (detail::encode_one(ps, std::forward<Args>(args)), ...);

        const int nParams = static_cast<int>(idx);

#if UPQ_REFLECT_DEBUG
        UPQ_CONN_DBG("SQL: %s", sql.c_str());
        UPQ_CONN_DBG("nParams=%d", nParams);
        for (int i = 0; i < nParams; ++i) {
            const char *v = values[i];
            UPQ_CONN_DBG("  $%d: type=%u fmt=%d len=%d val=%s",
                         i + 1,
                         static_cast<unsigned>(types[i]),
                         formats[i],
                         lengths[i],
                         v ? v : "NULL");
        }
#endif

        if (!PQsendQueryParams(conn_, sql.c_str(), nParams,
                               types.data(), values.data(), lengths.data(), formats.data(), 0)) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        for (;;) {
            const int fr = PQflush(conn_);
            if (fr == 0) break;
            if (fr == -1) {
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                connected_ = false;
                co_return out;
            }
            co_await wait_writable();
        }

        for (;;) {
            for (;;) {
                if (PQconsumeInput(conn_) == 0) {
                    out.code = PgErrorCode::SocketReadFailed;
                    out.error = PQerrorMessage(conn_);
                    connected_ = false;
                    co_return out;
                }

                bool saw_any = false;
                // PQgetResult blocks inside libpq (poll on the socket) while
                // the query is still executing server-side; only call it when
                // PQisBusy says a result is ready. Otherwise a slow query
                // (e.g. a contended pg_advisory_xact_lock) parks the whole
                // worker thread instead of suspending the coroutine.
                while (!PQisBusy(conn_)) {
                    PGresult *res = PQgetResult(conn_);
                    if (!res) break;
                    saw_any = true;
                    const auto st = PQresultStatus(res);

                    if (st == PGRES_TUPLES_OK) {
                        const int nrows = PQntuples(res);
                        const int ncols = PQnfields(res);

                        if (out.columns.empty()) {
                            out.columns.reserve(ncols);
                            for (int c = 0; c < ncols; ++c) {
                                const char *nm = PQfname(res, c);
                                out.columns.emplace_back(nm ? nm : "");
                            }
                        }

                        if (nrows > 0) out.rows.reserve(out.rows.size() + nrows);

                        for (int r = 0; r < nrows; ++r) {
                            QueryResult::Row row;
                            row.cols.reserve(ncols);

                            for (int c = 0; c < ncols; ++c) {
                                if (PQgetisnull(res, r, c)) {
                                    row.cols.emplace_back();
                                } else {
                                    const char *v = PQgetvalue(res, r, c);
                                    const int len = PQgetlength(res, r, c);
                                    row.cols.emplace_back(v, static_cast<size_t>(len));
                                }
                            }

                            out.rows.emplace_back(std::move(row));
                        }

                        out.ok = true;
                        out.code = PgErrorCode::OK;
                        if (out.rows_affected == 0) out.rows_affected = static_cast<uint64_t>(nrows);
                    } else if (st == PGRES_COMMAND_OK) {
                        out.ok = true;
                        out.code = PgErrorCode::OK;
                        out.rows_affected += extract_rows_affected(res);
                    } else {
                        fill_server_error_fields(res, out);
                    }

                    PQclear(res);
                }

                if (!PQisBusy(conn_)) {
                    if (saw_any && out.error.empty()) {
                        out.ok = true;
                        out.code = PgErrorCode::OK;
                    }
                    co_return out;
                }

                if (!sock_ || !sock_->get_raw_header()->has_unread_bytes()) {
                    if (sock_) sock_->get_raw_header()->disarm_read();
                    break;
                }
            }

            co_await wait_readable();
        }
    }

    template<class Sink>
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_to(const std::string &sql, Sink &&sink) {
        PgCopyResult start = co_await copy_out_start(sql);
        if (!start.ok) co_return start;

        PgCopyResult final_result{};
        final_result.ok = true;
        final_result.code = PgErrorCode::OK;

        bool user_aborted = false;

        for (;;) {
            auto chunk = co_await copy_out_read_chunk();
            if (!chunk.ok) {
                final_result.ok = false;
                final_result.code = chunk.err.code;
                final_result.error = chunk.err.message;
                co_return final_result;
            }
            if (chunk.value.empty()) {
                break;
            }
            if (user_aborted) {
                continue;
            }
            const std::string_view sv(
                reinterpret_cast<const char *>(chunk.value.data()),
                chunk.value.size());
            bool keep_going = co_await sink(sv);
            if (!keep_going) {
                user_aborted = true;
            }
        }

        if (user_aborted) {
            final_result.ok = false;
            final_result.code = PgErrorCode::AwaitCanceled;
            final_result.error = "COPY TO aborted by sink";
        }
        co_return final_result;
    }

    template<class LineSink>
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_to_lines(const std::string &sql,
                                     LineSink &&line_sink) {
        std::string carry;

        auto chunk_sink =
                [&](std::string_view sv) -> usub::uvent::task::Awaitable<bool> {
            size_t pos = 0;
            while (pos < sv.size()) {
                const auto nl = sv.find('\n', pos);
                if (nl == std::string_view::npos) {
                    carry.append(sv.data() + pos, sv.size() - pos);
                    break;
                }
                std::string_view line;
                if (!carry.empty()) {
                    carry.append(sv.data() + pos, nl - pos);
                    line = std::string_view(carry);
                } else {
                    line = std::string_view(sv.data() + pos, nl - pos);
                }
                bool keep = co_await line_sink(line);
                carry.clear();
                if (!keep) co_return false;
                pos = nl + 1;
            }
            co_return true;
        };

        PgCopyResult r = co_await copy_to(sql, chunk_sink);

        if (r.ok && !carry.empty()) {
            bool keep = co_await line_sink(std::string_view(carry));
            if (!keep) {
                r.ok = false;
                r.code = PgErrorCode::AwaitCanceled;
                r.error = "COPY TO aborted by sink";
            }
            carry.clear();
        }
        co_return r;
    }

    template<class Source>
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_from(const std::string &sql, Source &&source) {
        PgCopyResult start = co_await copy_in_start(sql);
        if (!start.ok) co_return start;

        for (;;) {
            std::string_view sv = co_await source();
            if (sv.empty()) break;
            PgCopyResult send = co_await copy_in_send_chunk(sv.data(), sv.size());
            if (!send.ok) {
                co_await copy_in_finish("aborted: send chunk failed");
                co_return send;
            }
        }
        co_return co_await copy_in_finish();
    }
} // namespace usub::pg

#endif // PGCONNECTIONLIBPQ_H
