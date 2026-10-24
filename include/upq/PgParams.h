#ifndef PGPARAMS_H
#define PGPARAMS_H

// Encoding of query arguments into libpq's PQsendQueryParams arrays.
//
// Scalars go binary (int2/int4/int8/float4/float8/bool), everything else as
// text with the type left to the server (types[i] = 0) unless an array/JSON
// OID is known. Text values coming from NUL-terminated storage the caller owns
// for the duration of the send (std::string, const char*, string literals) are
// referenced in place; values that need a terminator or are produced during
// encoding (string_view, enum tokens, JSON, array literals) are copied into
// the per-slot `owned` strings. Nothing here outlives the PQsendQueryParams
// call: libpq copies the data into its output buffer.

#include <array>
#include <bit>
#include <cassert>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>

#include <libpq-fe.h>

#include "PgJsonCodec.h"
#include "PgReflect.h"
#include "PgTypes.h"

namespace usub::pg {
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

    // Writable view over one parameter block (see ParamBlock). Every set_*
    // fills the next slot.
    struct ParamSlices {
        const char **values;
        int *lengths;
        int *formats;
        Oid *types;
        std::string *owned;   // one scratch string per slot
        uint64_t *bin;        // one 8-byte cell per slot for binary scalars
        size_t capacity;
        size_t count{0};

        size_t next_slot() {
            assert(count < capacity && "pg: more encoded parameters than counted");
            return count++;
        }

        void put(size_t i, const char *v, int len, int fmt, Oid oid) {
            values[i] = v;
            lengths[i] = len;
            formats[i] = fmt;
            types[i] = oid;
        }

        void set_null() { put(next_slot(), nullptr, 0, 0, 0); }

        // Text that is already NUL-terminated and stays alive until the send.
        void set_text_ref(const char *p, size_t n, Oid oid = 0) {
            put(next_slot(), p, static_cast<int>(n), 0, oid);
        }

        void set_text_typed(std::string_view sv, Oid oid) {
            const size_t i = next_slot();
            owned[i].assign(sv.data(), sv.size());
            put(i, owned[i].c_str(), static_cast<int>(sv.size()), 0, oid);
        }

        void set_text(std::string_view sv) { set_text_typed(sv, 0); }

        void set_text_owned(std::string &&s, Oid oid = 0) {
            const size_t i = next_slot();
            owned[i] = std::move(s);
            put(i, owned[i].c_str(), static_cast<int>(owned[i].size()), 0, oid);
        }

        void set_bin_raw(const void *data, size_t n, Oid oid) {
            const size_t i = next_slot();
            const char *p;
            if (n <= sizeof(uint64_t)) {
                std::memcpy(&bin[i], data, n);
                p = reinterpret_cast<const char *>(&bin[i]);
            } else {
                owned[i].assign(static_cast<const char *>(data), n);
                p = owned[i].data();
            }
            put(i, p, static_cast<int>(n), 1, oid);
        }

        template<class IntT>
        void set_bin_integral(IntT v, Oid oid) {
            if constexpr (sizeof(IntT) == 2) {
                const uint16_t be = to_be16(static_cast<uint16_t>(v));
                set_bin_raw(&be, 2, oid);
            } else if constexpr (sizeof(IntT) == 4) {
                const uint32_t be = to_be32(static_cast<uint32_t>(v));
                set_bin_raw(&be, 4, oid);
            } else {
                const uint64_t be = to_be64(static_cast<uint64_t>(v));
                set_bin_raw(&be, 8, oid);
            }
        }

        void set_bin_f32(float v) {
            const uint32_t be = fp_to_be(v);
            set_bin_raw(&be, 4, detail::FLOAT4OID);
        }

        void set_bin_f64(double v) {
            const uint64_t be = fp_to_be(v);
            set_bin_raw(&be, 8, detail::FLOAT8OID);
        }

        void set_bin_bool(bool v) {
            const unsigned char b = v ? 1u : 0u;
            set_bin_raw(&b, 1, detail::BOOLOID);
        }
    };

    namespace detail {
        // ---------- per-type encoders ----------
        //
        // Declared first so that tuple/aggregate/optional encoders can recurse
        // into any of them regardless of definition order.

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
            requires (is_tuple_like_v<std::decay_t<Tup> >
                      && !ArrayLike<std::decay_t<Tup> >
                      && !CArrayLike<std::decay_t<Tup> >)
        inline void encode_one(ParamSlices &ps, const Tup &tup);

        template<class T>
            requires (ReflectAggregate<std::decay_t<T> >
                      && !is_tuple_like_v<std::decay_t<T> >
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
        inline void encode_one(ParamSlices &ps, const std::string &v) { ps.set_text_ref(v.c_str(), v.size()); }

        template<size_t N>
        inline void encode_one(ParamSlices &ps, const char (&lit)[N]) {
            if constexpr (N == 0) {
                ps.set_text(std::string_view{});
            } else if (lit[N - 1] == '\0') {
                ps.set_text_ref(lit, N - 1);
            } else {
                ps.set_text(std::string_view(lit, N - 1));
            }
        }

        inline void encode_one(ParamSlices &ps, const char *v) {
            if (v) ps.set_text_ref(v, std::char_traits<char>::length(v));
            else ps.set_null();
        }

        template<EnumType E>
        inline void encode_one(ParamSlices &ps, E v) {
            std::string tok;
            if (enum_to_token_impl<E>(v, tok)) {
                ps.set_text_owned(std::move(tok));
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
            if (!p.ptr) {
                ps.set_null();
                return;
            }
            ps.set_text_owned(::usub::pg::json::dump(*p.ptr), Jsonb ? JSONBOID : JSONOID);
        }

        template<class T, bool Strict>
        inline void encode_one(ParamSlices &ps, const ::usub::pg::PgJson<T, Strict> &v) {
            encode_one(ps, ::usub::pg::pg_jsonb<T, Strict>(v.value));
        }

        // ---------- array literals ('{a,"b",NULL}') ----------

        inline void append_escaped_array_elem(std::string &s, std::string_view in) {
            s.push_back('"');
            for (char ch : in) {
                if (ch == '"' || ch == '\\') s.push_back('\\');
                s.push_back(ch);
            }
            s.push_back('"');
        }

        // Kept for source compatibility; append_escaped_array_elem avoids the
        // temporary.
        inline std::string escape_pg_array_elem(std::string_view in) {
            std::string out;
            out.reserve(in.size() + 4);
            for (char ch : in) {
                if (ch == '"' || ch == '\\') out.push_back('\\');
                out.push_back(ch);
            }
            return out;
        }

        // Shortest representation that round-trips (std::to_chars), so float
        // arrays no longer lose digits to ostream's default precision of 6.
        template<class T>
        inline void append_number(std::string &s, T v) {
            char buf[64];
            auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), v);
            if (ec == std::errc{}) s.append(buf, p);
        }

        template<class T>
        inline std::string to_text_number(T v) {
            std::string s;
            append_number(s, v);
            return s;
        }

        template<class Elem>
        inline void append_pg_array_elem(std::string &s, const Elem &v) {
            if constexpr (EnumType<Elem>) {
                std::string tok;
                if (!enum_to_token_impl<Elem>(v, tok)) {
                    using U = std::underlying_type_t<Elem>;
                    tok = std::to_string(static_cast<U>(v));
                }
                append_escaped_array_elem(s, tok);
            } else if constexpr (std::is_same_v<std::decay_t<Elem>, bool>) {
                s.append(v ? "true" : "false");
            } else if constexpr (StringLike<Elem> || std::is_same_v<std::decay_t<Elem>, std::string_view>) {
                append_escaped_array_elem(s, std::string_view(v));
            } else if constexpr (CharPtr<Elem>) {
                append_escaped_array_elem(s, v ? std::string_view(v) : std::string_view{});
            } else if constexpr (Integral<Elem>) {
                append_number(s, v);
            } else if constexpr (Floating<Elem>) {
                append_number(s, static_cast<double>(v));
            } else {
                std::ostringstream oss;
                oss << v;
                append_escaped_array_elem(s, oss.str());
            }
        }

        template<class Range>
        inline std::string build_pg_text_array_literal_any(const Range &cont) {
            std::string s;
            s.push_back('{');
            bool first = true;
            for (auto &&e0 : cont) {
                if (!first) s.push_back(',');
                first = false;
                using E0 = std::decay_t<decltype(e0)>;
                if constexpr (Optional<E0>) {
                    if (!e0) s.append("NULL");
                    else append_pg_array_elem<unopt_t<E0> >(s, *e0);
                } else {
                    append_pg_array_elem<E0>(s, e0);
                }
            }
            s.push_back('}');
            return s;
        }

        template<class Elem>
        consteval Oid array_oid_for_elem() {
            return pick_array_oid<Elem>();
        }

        template<class Elem, class Range>
        inline void encode_array(ParamSlices &ps, const Range &r) {
            ps.set_text_owned(build_pg_text_array_literal_any(r), array_oid_for_elem<unopt_t<Elem> >());
        }

        template<ArrayLike C>
        inline void encode_one(ParamSlices &ps, const C &cont) {
            encode_array<typename std::decay_t<C>::value_type>(ps, cont);
        }

        template<class T, class A>
        inline void encode_one(ParamSlices &ps, const std::vector<T, A> &cont) {
            encode_array<T>(ps, cont);
        }

        template<class T, size_t N>
        inline void encode_one(ParamSlices &ps, const T (&arr)[N]) {
            struct PtrRange {
                const T *b;
                const T *e;
                const T *begin() const { return b; }
                const T *end() const { return e; }
            } r{arr, arr + N};
            encode_array<T>(ps, r);
        }

        template<class T, size_t N>
        inline void encode_one(ParamSlices &ps, T (&arr)[N]) {
            encode_one(ps, const_cast<const T(&)[N]>(arr));
        }

        template<class T>
        inline void encode_one(ParamSlices &ps, std::initializer_list<T> il) {
            encode_array<T>(ps, il);
        }

        template<class Tup>
            requires (is_tuple_like_v<std::decay_t<Tup> >
                      && !ArrayLike<std::decay_t<Tup> >
                      && !CArrayLike<std::decay_t<Tup> >)
        inline void encode_one(ParamSlices &ps, const Tup &tup) {
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (encode_one(ps, std::get<I>(tup)), ...);
            }(std::make_index_sequence<std::tuple_size_v<std::decay_t<Tup> > >{});
        }

        template<class T>
            requires (ReflectAggregate<std::decay_t<T> >
                      && !is_tuple_like_v<std::decay_t<T> >
                      && !ArrayLike<std::decay_t<T> >
                      && !CArrayLike<std::decay_t<T> >)
        inline void encode_one(ParamSlices &ps, const T &obj) {
            using V = std::decay_t<T>;
            auto tiev = ureflect::to_tie(const_cast<V &>(obj));
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
            if constexpr (std::is_convertible_v<T, std::string_view>) {
                ps.set_text(std::string_view(v));
            } else if constexpr (std::is_constructible_v<std::string, T>) {
                ps.set_text_owned(std::string(std::forward<T>(v)));
            } else if constexpr (Streamable<T>) {
                std::ostringstream oss;
                oss << v;
                ps.set_text_owned(oss.str());
            } else {
                std::ostringstream oss;
                oss << "<param:" << typeid(std::decay_t<T>).name() << ">";
                ps.set_text_owned(oss.str());
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

        // ---------- slot counting ----------

        template<class T>
        struct param_arity_impl {
            static constexpr size_t value = 1;
        };

        template<class Opt>
            requires Optional<std::decay_t<Opt> >
        struct param_arity_impl<Opt> {
            static constexpr size_t value = param_arity_impl<typename std::decay_t<Opt>::value_type>::value;
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
            requires (is_tuple_like_v<std::decay_t<Tup> >
                      && !ArrayLike<std::decay_t<Tup> >
                      && !CArrayLike<std::decay_t<Tup> >)
        struct param_arity_impl<Tup> {
            static constexpr size_t value = std::tuple_size_v<std::decay_t<Tup> >;
        };

        template<class T>
            requires (ReflectAggregate<std::decay_t<T> >
                      && !is_tuple_like_v<std::decay_t<T> >
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

        // Highest $N placeholder in the statement.
        constexpr size_t count_pg_params(std::string_view sql) {
            size_t max_param = 0;
            for (size_t i = 0; i + 1 < sql.size(); ++i) {
                if (sql[i] != '$') continue;
                size_t num = 0;
                size_t j = i + 1;
                while (j < sql.size() && sql[j] >= '0' && sql[j] <= '9') {
                    num = num * 10 + static_cast<size_t>(sql[j] - '0');
                    ++j;
                }
                if (j > i + 1 && num > max_param) max_param = num;
            }
            return max_param;
        }

        // Debug-build guard used by every string_view-SQL overload.
        template<class... Args>
        inline void assert_param_count([[maybe_unused]] std::string_view sql) {
            assert(count_pg_params(sql) == count_total_params<Args...>() &&
                "pg: $N placeholder count does not match argument count");
        }

        // ---------- the block itself ----------

        // Stack storage for one PQsendQueryParams call with N slots.
        template<size_t N>
        struct ParamBlock {
            static constexpr size_t Cap = N ? N : 1;
            const char *values[Cap]{};
            int lengths[Cap]{};
            int formats[Cap]{};
            Oid types[Cap]{};
            std::string owned[Cap];
            uint64_t bin[Cap]{};

            ParamSlices slices() { return ParamSlices{values, lengths, formats, types, owned, bin, Cap}; }
        };

        // Encodes args and queues the statement on the connection. Returns
        // PQsendQueryParams' verdict (0 = failed, see PQerrorMessage).
        template<class... Args>
        inline int send_query_params(PGconn *conn, const char *sql, const Args &... args) {
            ParamBlock<count_total_params<Args...>()> block;
            ParamSlices ps = block.slices();
            (encode_one(ps, args), ...);
            return PQsendQueryParams(conn, sql, static_cast<int>(ps.count), block.types, block.values,
                                     block.lengths, block.formats, 0);
        }
    } // namespace detail
} // namespace usub::pg

#endif // PGPARAMS_H
