#ifndef PGREFLECT_H
#define PGREFLECT_H

#include <ureflect/ureflect_auto.h>

#include "PgJsonCodec.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ios>
#include <istream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "PgTypes.h"

#ifndef UPQ_REFLECT_DEBUG
#define UPQ_REFLECT_DEBUG 0
#endif

#if UPQ_REFLECT_DEBUG
#ifndef UPQ_LOG
#define UPQ_LOG(fmt, ...) std::printf(fmt "\n", ##__VA_ARGS__)
#endif
#else
#ifndef UPQ_LOG
#define UPQ_LOG(...) ((void)0)
#endif
#endif

namespace usub::pg {
    namespace detail {
        inline std::string join_csv(const std::vector<std::string> &v) {
            std::string out;
            out.reserve(64);
            for (size_t i = 0; i < v.size(); ++i) {
                if (i) out += ", ";
                out += v[i];
            }
            return out;
        }

        inline std::string normalize_ident(std::string_view in) {
            std::string s;
            s.reserve(in.size());
            for (char ch : in) {
                unsigned char c = static_cast<unsigned char>(ch);
                if (std::isalnum(c))
                    s.push_back(static_cast<char>(std::tolower(c)));
                else if (ch == '_')
                    s.push_back('_');
            }
            std::string out;
            out.reserve(s.size());
            bool prev_us = false;
            for (char ch : s) {
                if (ch == '_') {
                    if (!prev_us) out.push_back('_');
                    prev_us = true;
                } else {
                    out.push_back(ch);
                    prev_us = false;
                }
            }
            return out;
        }

        template <class T, class = void>
        struct is_tuple_like : std::false_type {};

        template <class T>
        struct is_tuple_like<T, std::void_t<decltype(std::tuple_size<std::decay_t<T>>::value)>>
            : std::true_type {};

        template <class T>
        inline constexpr bool is_tuple_like_v = is_tuple_like<T>::value;

        template <class T>
        concept ReflectAggregate =
            std::is_aggregate_v<std::decay_t<T>> && (ureflect::count_members<std::decay_t<T>> > 0);

        inline bool fully_consumed(std::istringstream &iss) {
            iss >> std::ws;
            return iss.peek() == std::char_traits<char>::eof();
        }

        template <class Int>
        inline bool parse_int(std::string_view sv, Int &out) noexcept {
            Int tmp{};
            const char *begin = sv.data();
            const char *end = sv.data() + sv.size();
            auto res = std::from_chars(begin, end, tmp);
            if (res.ec == std::errc() && res.ptr == end) {
                out = tmp;
                return true;
            }
            return false;
        }

        inline bool parse_bool(std::string_view sv, bool &out) noexcept {
            if (sv == "t" || sv == "true" || sv == "1") {
                out = true;
                return true;
            }
            if (sv == "f" || sv == "false" || sv == "0") {
                out = false;
                return true;
            }
            return false;
        }

        template <class Float>
        inline bool parse_float(std::string_view sv, Float &out) noexcept {
            if (sv.empty()) return false;
            Float tmp{};
            const char *begin = sv.data();
            const char *end = sv.data() + sv.size();
            auto res = std::from_chars(begin, end, tmp);
            if (res.ec == std::errc() && res.ptr == end) {
                out = tmp;
                return true;
            }
            return false;
        }

        inline std::vector<std::string_view> split_pg_array_items(std::string_view s) {
            std::vector<std::string_view> items;
            if (s.size() < 2 || s.front() != '{' || s.back() != '}') return items;
            size_t i = 1, start = 1;
            bool inq = false;
            for (; i + 1 < s.size(); ++i) {
                char c = s[i];
                if (inq) {
                    if (c == '\\' && i + 1 < s.size()) {
                        ++i;
                        continue;
                    }
                    if (c == '"') {
                        inq = false;
                        continue;
                    }
                } else {
                    if (c == '"') {
                        inq = true;
                        continue;
                    }
                    if (c == ',') {
                        items.emplace_back(s.substr(start, i - start));
                        start = i + 1;
                    }
                }
            }
            if (start <= s.size() - 1) items.emplace_back(s.substr(start, s.size() - 1 - start));
            return items;
        }

        inline bool parse_pg_text_elt(std::string_view sv, std::string &out, bool &is_null) {
            is_null = false;
            if (sv == "NULL") {
                is_null = true;
                out.clear();
                return true;
            }
            if (!sv.empty() && sv.front() == '"' && sv.back() == '"') {
                out.clear();
                out.reserve(sv.size());
                for (size_t i = 1; i + 1 < sv.size(); ++i) {
                    char c = sv[i];
                    if (c == '\\' && i + 1 < sv.size() - 1) {
                        out.push_back(sv[i + 1]);
                        ++i;
                    } else {
                        out.push_back(c);
                    }
                }
                return true;
            }
            out.assign(sv.begin(), sv.end());
            return true;
        }

        inline bool parse_pg_numrange_parts(std::string_view sv, bool &lo_inc, bool &hi_inc,
                                            std::string_view &lo, std::string_view &hi,
                                            bool &is_empty) noexcept {
            is_empty = false;
            if (sv == "empty") {
                is_empty = true;
                return true;
            }
            if (sv.size() < 2) return false;

            const char lb = sv.front();
            const char rb = sv.back();
            if (lb == '[')
                lo_inc = true;
            else if (lb == '(')
                lo_inc = false;
            else
                return false;

            if (rb == ']')
                hi_inc = true;
            else if (rb == ')')
                hi_inc = false;
            else
                return false;

            sv.remove_prefix(1);
            sv.remove_suffix(1);

            const auto comma = sv.find(',');
            if (comma == std::string_view::npos) return false;

            lo = sv.substr(0, comma);
            hi = sv.substr(comma + 1);

            auto trim = [](std::string_view x) {
                while (!x.empty() && (x.front() == ' ' || x.front() == '\t' || x.front() == '\n' ||
                                      x.front() == '\r'))
                    x.remove_prefix(1);
                while (!x.empty() && (x.back() == ' ' || x.back() == '\t' || x.back() == '\n' ||
                                      x.back() == '\r'))
                    x.remove_suffix(1);
                return x;
            };
            lo = trim(lo);
            hi = trim(hi);

            return true;
        }

        template <class T, class = void>
        struct has_istream_extractor : std::false_type {};

        template <class T>
        struct has_istream_extractor<
            T, std::void_t<decltype(std::declval<std::istream &>() >> std::declval<T &>())>>
            : std::true_type {};

        inline std::string preview(std::string_view sv, size_t limit = 80) {
            if (sv.size() <= limit) return std::string(sv);
            std::string s;
            s.reserve(limit + 3);
            s.append(sv.data(), limit);
            s += "...";
            return s;
        }

        template <class T>
        inline std::string type_name_short() {
            std::string s{std::string(ureflect::type_name<T>())};
            auto pos = s.rfind("::");
            return (pos == std::string::npos) ? s : s.substr(pos + 2);
        }

        inline std::string pg_type_name_from_oid(uint32_t oid) {
#ifdef UPQ_HAVE_OID_TO_TYPENAME
            return oid_to_typename(oid);
#else
            (void)oid;
            return "unknown";
#endif
        }

        inline std::string preview_val(std::string_view sv) { return preview(sv, 80); }

        template <class T>
        inline std::string expect_type() {
            return type_name_short<T>();
        }

        inline std::string format_mismatch_named(std::string_view field_name,
                                                 std::string_view field_type,
                                                 std::string_view col_name,
                                                 std::string_view col_type,
                                                 std::string_view val_preview) {
            std::string out;
            out.reserve(160);
            out += "decode failed: field='";
            out += field_name;
            out += "' (type=";
            out += field_type;
            out += ") \xE2\x86\x90 column='";
            out += col_name;
            out += "' (type=";
            out += col_type;
            out += "): expected=";
            out += field_type;
            out += ", got=\"";
            out += std::string(val_preview);
            out += "\"";
            return out;
        }

        inline std::string format_mismatch_positional(size_t field_index,
                                                      std::string_view field_type, size_t col_index,
                                                      std::string_view col_name,
                                                      std::string_view col_type,
                                                      std::string_view val_preview) {
            std::string out;
            out.reserve(200);
            out += "decode failed: field#";
            out += std::to_string(field_index);
            out += " (type=";
            out += field_type;
            out += ") \xE2\x86\x90 column#";
            out += std::to_string(col_index);
            if (!col_name.empty()) {
                out += " '";
                out += col_name;
                out += "'";
            }
            out += " (type=";
            out += col_type;
            out += "): expected=";
            out += field_type;
            out += ", got=\"";
            out += std::string(val_preview);
            out += "\"";
            return out;
        }

        template <class T, class Enable = void>
        struct Decoder {
            static bool apply(std::string_view sv, T &out) {
                if constexpr (std::is_same_v<T, std::string>) {
                    out.assign(sv.data(), sv.size());
                    return true;
                } else if constexpr (std::is_same_v<T, std::string_view>) {
                    out = sv;
                    return true;
                } else if constexpr (std::is_same_v<T, bool>) {
                    return parse_bool(sv, out);
                } else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
                    return parse_int<T>(sv, out);
                } else if constexpr (std::is_floating_point_v<T>) {
                    return parse_float<T>(sv, out);
                } else if constexpr (has_istream_extractor<T>::value) {
                    std::istringstream iss{std::string(sv)};
                    T tmp{};
                    if ((iss >> tmp) && fully_consumed(iss)) {
                        out = std::move(tmp);
                        return true;
                    }
                    return false;
                } else if constexpr (std::is_enum_v<T>) {
                    return detail::enum_from_token_impl(sv, out);
                } else {
                    return false;
                }
            }
        };

        template <class T>
        struct Decoder<std::optional<T>, void> {
            static bool apply(std::string_view sv, std::optional<T> &out) {
                if (sv.size() == 0) {
                    out.reset();
                    return true;
                }
                T v{};
                if (Decoder<T>::apply(sv, v)) {
                    out = std::move(v);
                    return true;
                }
                return false;
            }
        };

        template <class T>
        struct is_std_vector_reflect : std::false_type {};

        template <class U, class A>
        struct is_std_vector_reflect<std::vector<U, A>> : std::true_type {};

        template <class VecT>
        struct Decoder<VecT, std::enable_if_t<is_std_vector_reflect<VecT>::value>> {
            using T = typename VecT::value_type;

            static bool apply(std::string_view sv, VecT &out) {
                out.clear();
                auto parts = split_pg_array_items(sv);
                if (parts.empty() && !(sv.size() >= 2 && sv.front() == '{' && sv.back() == '}'))
                    return false;

                out.reserve(parts.size());
                for (auto p : parts) {
                    std::string tok;
                    bool is_null = false;
                    if (!parse_pg_text_elt(p, tok, is_null)) return false;
                    if (is_null) {
                        out.emplace_back(T{});
                        continue;
                    }

                    if constexpr (std::is_same_v<T, std::string>)
                        out.emplace_back(std::move(tok));
                    else if constexpr (std::is_integral_v<T>) {
                        T v{};
                        if (!parse_int<T>(tok, v)) return false;
                        out.emplace_back(v);
                    } else if constexpr (std::is_floating_point_v<T>) {
                        T v{};
                        if (!parse_float<T>(tok, v)) return false;
                        out.emplace_back(v);
                    } else if constexpr (has_istream_extractor<T>::value) {
                        std::istringstream iss(tok);
                        T v{};
                        if (!(iss >> v) || !fully_consumed(iss)) return false;
                        out.emplace_back(std::move(v));
                    } else if constexpr (std::is_enum_v<T>) {
                        T v{};
                        if (!detail::enum_from_token_impl(tok, v)) return false;
                        out.emplace_back(v);
                    } else {
                        return false;
                    }
                }
                return true;
            }
        };

        template <class T>
        struct Decoder<
            T, std::enable_if_t<ReflectAggregate<T> && !::usub::pg::detail::is_pg_json_v<T> &&
                                !::usub::pg::detail::is_pg_numrange_reflect_v<T>>> {
            static bool apply(std::string_view, T &) { return false; }
        };

        template <class T, bool Strict>
        struct Decoder<::usub::pg::PgJson<T, Strict>, void> {
            static bool apply(std::string_view sv, ::usub::pg::PgJson<T, Strict> &out) {
                if (sv.empty()) return false;
                auto r = ::usub::pg::json::parse<T, Strict>(sv);
                if (!r) return false;
                out.value = std::move(*r);
                return true;
            }
        };

        template <class T>
        struct Decoder<::usub::pg::PgNumRange<T>, void> {
            static bool apply(std::string_view sv, ::usub::pg::PgNumRange<T> &out) {
                out = {};

                bool lo_inc = true, hi_inc = false, is_empty = false;
                std::string_view lo_s, hi_s;
                if (!parse_pg_numrange_parts(sv, lo_inc, hi_inc, lo_s, hi_s, is_empty))
                    return false;

                out.lo_inclusive = lo_inc;
                out.hi_inclusive = hi_inc;
                out.empty = is_empty;

                if (is_empty) {
                    out.lo.reset();
                    out.hi.reset();
                    return true;
                }

                if (!lo_s.empty()) {
                    T v{};
                    if (!Decoder<T>::apply(lo_s, v)) return false;
                    out.lo = std::move(v);
                }

                if (!hi_s.empty()) {
                    T v{};
                    if (!Decoder<T>::apply(hi_s, v)) return false;
                    out.hi = std::move(v);
                }

                return true;
            }
        };

#ifdef UPQ_ENABLE_PARAM_ENCODER
        inline void append_escaped(std::string &dst, std::string_view s) {
            dst.push_back('"');
            for (char c : s) {
                if (c == '"')
                    dst += "\"\"";
                else
                    dst.push_back(c);
            }
            dst.push_back('"');
        }

        template <class T, class Enable = void>
        struct ToPg;

        template <class T>
        struct ToPg<T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>>> {
            static void append(std::string &dst, T v) { dst += std::to_string(v); }
        };

        template <>
        struct ToPg<bool, void> {
            static void append(std::string &dst, bool v) { dst += (v ? "true" : "false"); }
        };

        template <class T>
        struct ToPg<T, std::enable_if_t<std::is_floating_point_v<T>>> {
            static void append(std::string &dst, T v) {
                std::ostringstream oss;
                oss.setf(std::ios::fmtflags(0), std::ios::floatfield);
                oss.precision(17);
                oss << v;
                dst += oss.str();
            }
        };

        template <>
        struct ToPg<std::string, void> {
            static void append(std::string &dst, const std::string &v) { append_escaped(dst, v); }
        };

        template <>
        struct ToPg<std::string_view, void> {
            static void append(std::string &dst, std::string_view v) { append_escaped(dst, v); }
        };

        template <class T>
        struct ToPg<std::optional<T>, void> {
            static void append(std::string &dst, const std::optional<T> &v) {
                if (!v) {
                    dst += "NULL";
                    return;
                }
                ToPg<T>::append(dst, *v);
            }
        };

        template <class Vec>
        struct ToPg<Vec, std::enable_if_t<is_std_vector<Vec>::value>> {
            using T = typename Vec::value_type;

            static void append(std::string &dst, const Vec &v) {
                dst.push_back('{');
                for (size_t i = 0; i < v.size(); ++i) {
                    if (i) dst.push_back(',');
                    if constexpr (std::is_same_v<T, std::string> ||
                                  std::is_same_v<T, std::string_view>)
                        append_escaped(dst, std::string_view(v[i]));
                    else
                        ToPg<T>::append(dst, v[i]);
                }
                dst.push_back('}');
            }
        };

        template <class Agg>
            requires ReflectAggregate<Agg>
        inline void reflect_to_param_strings(const Agg &a, std::vector<std::string> &out) {
            auto tie = ureflect::to_tie(const_cast<Agg &>(a));
            constexpr size_t N = ureflect::count_members<Agg>;
            out.reserve(out.size() + N);
            [&]<size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using F = std::remove_reference_t<decltype(ureflect::get<I>(tie))>;
                        std::string s;
                        s.reserve(32);
                        ToPg<F>::append(s, ureflect::get<I>(tie));
                        out.emplace_back(std::move(s));
                    }(),
                    ...);
            }(std::make_index_sequence<N>{});
        }
        template <class E>
        struct ToPg<E, std::enable_if_t<std::is_enum_v<E>>> {
            static void append(std::string &dst, E v) {
                std::string tok;
                if (::usub::pg::detail::enum_to_token_impl(v, tok)) {
                    append_escaped(dst, tok);
                } else {
                    using U = std::underlying_type_t<E>;
                    dst += std::to_string(static_cast<long long>(static_cast<U>(v)));
                }
            }
        };
#endif  // UPQ_ENABLE_PARAM_ENCODER

        inline int find_col_idx(const std::vector<std::string> &cols, std::string_view norm_name) {
            for (size_t i = 0; i < cols.size(); ++i)
                if (cols[i] == norm_name) return static_cast<int>(i);
            return -1;
        }

        template <class Tuple>
            requires is_tuple_like_v<Tuple>
        inline bool fill_from_row_positional_ex(const QueryResult::Row &row,
                                                const std::vector<std::string> *col_names,
                                                const std::vector<uint32_t> *col_oids, Tuple &dst,
                                                std::string *err) {
            using Tup = std::decay_t<Tuple>;
            constexpr std::size_t N = std::tuple_size_v<Tup>;
            if (row.cols.size() < N) {
                if (err)
                    *err = "not enough columns: expected=" + std::to_string(N) +
                           ", got=" + std::to_string(row.cols.size());
                return false;
            }

            bool ok = true;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using Elem = std::tuple_element_t<I, Tup>;
                        const auto &sv = row.cols[I];
                        Elem tmp{};

                        std::string col_type = "unknown";
                        std::string col_name;
                        if (col_oids && I < col_oids->size())
                            col_type = pg_type_name_from_oid((*col_oids)[I]);
                        if (col_names && I < col_names->size()) col_name = (*col_names)[I];

                        if (!Decoder<Elem>::apply(std::string_view(sv.data(), sv.size()), tmp)) {
                            if (err)
                                *err = format_mismatch_positional(
                                    I, expect_type<Elem>(), I, col_name, col_type,
                                    preview_val(std::string_view(sv.data(), sv.size())));
                            ok = false;
                        } else {
                            std::get<I>(dst) = std::move(tmp);
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<N>{});
            if (!ok && err && err->empty()) *err = "failed to decode tuple element";
            return ok;
        }

        template <class T>
            requires ReflectAggregate<T>
        inline bool fill_from_row_positional_ex(const QueryResult::Row &row,
                                                const std::vector<std::string> *col_names,
                                                const std::vector<uint32_t> *col_oids, T &dst,
                                                std::string *err) {
            using V = std::decay_t<T>;
            constexpr std::size_t N = ureflect::count_members<V>;
            if (row.cols.size() < N) {
                if (err)
                    *err = "not enough columns: expected=" + std::to_string(N) +
                           ", got=" + std::to_string(row.cols.size());
                return false;
            }

            auto tie = ureflect::to_tie(dst);
            bool ok = true;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using FieldT = std::remove_reference_t<decltype(ureflect::get<I>(tie))>;
                        const auto &sv = row.cols[I];
                        FieldT tmp{};

                        std::string col_type = "unknown";
                        std::string col_name;
                        if (col_oids && I < col_oids->size())
                            col_type = pg_type_name_from_oid((*col_oids)[I]);
                        if (col_names && I < col_names->size()) col_name = (*col_names)[I];

                        if (!Decoder<FieldT>::apply(std::string_view(sv.data(), sv.size()), tmp)) {
                            if (err)
                                *err = format_mismatch_positional(
                                    I, expect_type<FieldT>(), I, col_name, col_type,
                                    preview_val(std::string_view(sv.data(), sv.size())));
                            ok = false;
                        } else {
                            ureflect::get<I>(tie) = std::move(tmp);
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<N>{});
            if (!ok && err && err->empty()) *err = "failed to decode aggregate field (positional)";
            return ok;
        }

        template <class T>
            requires ReflectAggregate<T>
        inline bool fill_from_row_named(const QueryResult &qr, size_t row_index, T &dst,
                                        std::string *err) {
            using V = std::decay_t<T>;
            constexpr std::size_t N = ureflect::count_members<V>;

            if (row_index >= qr.rows.size()) {
                if (err)
                    *err = "row out of range: row=" + std::to_string(row_index) +
                           ", total_rows=" + std::to_string(qr.rows.size());
                return false;
            }
            const auto &row = qr.rows[row_index];

            if (qr.columns.empty()) {
                if (err) *err = "columns are empty (driver didn't fill names)";
                return false;
            }

            std::vector<std::string> norm_cols;
            norm_cols.reserve(qr.columns.size());
            for (auto &c : qr.columns) norm_cols.emplace_back(normalize_ident(c));

#if UPQ_REFLECT_DEBUG
            UPQ_LOG("[UPQ/reflect] columns[%zu]: %s", norm_cols.size(),
                    join_csv(qr.columns).c_str());
#endif

            constexpr auto fnames = ureflect::member_names<V>;
            std::array<std::string, N> norm_fields{};
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                ((norm_fields[I] = normalize_ident(fnames[I])), ...);
            }(std::make_index_sequence<N>{});

            int col_map[N];
            bool all_found = true;
            std::vector<std::string> missing;

            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (([&] {
                     const int idx = find_col_idx(norm_cols, norm_fields[I]);
                     col_map[I] = idx;
                     if (idx < 0) {
                         all_found = false;
                         missing.emplace_back(norm_fields[I]);
                     }
                 }()),
                 ...);
            }(std::make_index_sequence<N>{});

            if (!all_found) {
                if (err) {
                    *err = "not all fields matched by name: missing=[" + join_csv(missing) +
                           "], available_cols=[" + join_csv(qr.columns) + "]";
                }
                return false;
            }

            auto tie = ureflect::to_tie(dst);
            bool ok = true;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using FieldT = std::remove_reference_t<decltype(ureflect::get<I>(tie))>;
                        const int c = col_map[I];
                        const auto &sv = row.cols[static_cast<size_t>(c)];
                        FieldT tmp{};

                        std::string col_type = "unknown";
#ifdef UPQ_RESULT_HAS_COLUMN_OIDS
                        if (c >= 0 && static_cast<size_t>(c) < qr.column_oids.size())
                            col_type =
                                pg_type_name_from_oid(qr.column_oids[static_cast<size_t>(c)]);
#endif

                        if (!Decoder<FieldT>::apply(std::string_view(sv.data(), sv.size()), tmp)) {
                            if (err) {
                                *err = format_mismatch_named(
                                    fnames[I], expect_type<FieldT>(),
                                    qr.columns[static_cast<size_t>(c)], col_type,
                                    preview_val(std::string_view(sv.data(), sv.size())));
                            }
                            ok = false;
                        } else {
                            ureflect::get<I>(tie) = std::move(tmp);
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<N>{});

            if (!ok && err && err->empty()) *err = "failed to decode aggregate field (named)";
            return ok;
        }
    }  // namespace detail

    template <class T>
    inline bool map_row_reflect_positional(const QueryResult::Row &row, T &out,
                                           std::string *err = nullptr) {
        std::string local;
        std::string *perr = err ? err : &local;
        return detail::fill_from_row_positional_ex(row, nullptr, nullptr, out, perr);
    }

    template <class T>
    inline bool map_row_reflect_positional_ex(const QueryResult &qr, size_t row_index, T &out,
                                              std::string *err = nullptr) {
        if (row_index >= qr.rows.size()) {
            if (err) *err = "row out of range";
            return false;
        }
        const auto &row = qr.rows[row_index];
        const std::vector<std::string> *names = qr.columns.empty() ? nullptr : &qr.columns;
#ifdef UPQ_RESULT_HAS_COLUMN_OIDS
        const std::vector<uint32_t> *oids = &qr.column_oids;
#else
        const std::vector<uint32_t> *oids = nullptr;
#endif
        std::string local;
        std::string *perr = err ? err : &local;
        return detail::fill_from_row_positional_ex(row, names, oids, out, perr);
    }

    template <class T>
    inline T map_single_reflect_positional(const QueryResult &qr, size_t row = 0,
                                           std::string *err = nullptr) {
        std::string local;
        std::string *perr = err ? err : &local;
        T dst{};
        if (!map_row_reflect_positional_ex(qr, row, dst, perr))
            throw std::runtime_error(!perr->empty() ? *perr : "decode failed");
        return dst;
    }

    template <class T>
    inline std::vector<T> map_all_reflect_positional(const QueryResult &qr,
                                                     std::string *err = nullptr) {
        std::string local;
        std::string *perr = err ? err : &local;
        std::vector<T> out;
        out.reserve(qr.rows.size());
        for (size_t i = 0; i < qr.rows.size(); ++i) {
            T dst{};
            if (!map_row_reflect_positional_ex(qr, i, dst, perr)) {
                if (perr->empty()) *perr = "decode failed";
                *perr = "row=" + std::to_string(i) + ": " + *perr;
                throw std::runtime_error(*perr);
            }
            out.emplace_back(std::move(dst));
        }
        return out;
    }

    template <class T>
        requires detail::ReflectAggregate<T>
    inline bool map_row_reflect_named(const QueryResult &qr, size_t row_index, T &out,
                                      std::string *err = nullptr) {
        std::string local;
        std::string *perr = err ? err : &local;
        return detail::fill_from_row_named(qr, row_index, out, perr);
    }

    template <class T>
        requires detail::ReflectAggregate<T>
    inline T map_single_reflect_named(const QueryResult &qr, size_t row = 0,
                                      std::string *err = nullptr) {
        std::string local;
        std::string *perr = err ? err : &local;
        T dst{};
        if (!map_row_reflect_named(qr, row, dst, perr))
            throw std::runtime_error(!perr->empty() ? *perr : "decode failed");
        return dst;
    }

    template <class T>
        requires detail::ReflectAggregate<T>
    inline std::vector<T> map_all_reflect_named(const QueryResult &qr, std::string *err = nullptr) {
        std::string local;
        std::string *perr = err ? err : &local;
        std::vector<T> out;
        out.reserve(qr.rows.size());
        for (size_t i = 0; i < qr.rows.size(); ++i) {
            T dst{};
            if (!map_row_reflect_named(qr, i, dst, perr)) {
                if (perr->empty()) *perr = "decode failed";
                *perr = "row=" + std::to_string(i) + ": " + *perr;
                throw std::runtime_error(*perr);
            }
            out.emplace_back(std::move(dst));
        }
        return out;
    }
}  // namespace usub::pg

#endif  // PGREFLECT_H
