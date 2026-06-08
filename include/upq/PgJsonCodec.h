#ifndef UPQ_PG_JSON_CODEC_H
#define UPQ_PG_JSON_CODEC_H

// =============================================================================
// Pluggable JSON backend for UPQ.
//
// UPQ (de)serializes C++ structs to/from JSON in exactly two operations:
//
//     std::string                       dump(const T&);            // C++ -> JSON text
//     std::expected<T, std::string>     parse<T, Strict>(sv);      // JSON text -> C++
//
// Everything in the library funnels through `usub::pg::json::dump` /
// `usub::pg::json::parse`, so the actual JSON engine is swappable.
//
// Three ways to customize -- all resolved at compile time:
//
//   1) Pick a BUILT-IN global backend (works "out of the box", no per-type code):
//
//        ujson    (default) -- https://github.com/Usub-Foundation/ujson
//        glaze              -- define UPQ_JSON_BACKEND_GLAZE
//        nlohmann/json      -- define UPQ_JSON_BACKEND_NLOHMANN
//
//      The matching third-party header is only #included when its backend is
//      selected, so glaze / nlohmann are NEVER a hard dependency of UPQ.
//
//   2) Plug in YOUR OWN global backend, entirely from code -- no CMake edits,
//      no edits to this file. Define a backend type and point UPQ at it:
//
//        namespace my { struct JsonBackend {
//            static constexpr std::string_view name = "my";
//            template <class T> static std::string dump(const T& v);
//            template <class T, bool Strict>
//            static std::expected<T, std::string> parse(std::string_view sv);
//        }; }
//        #define UPQ_JSON_BACKEND_CUSTOM ::my::JsonBackend
//
//   3) Override a SINGLE type, regardless of the selected backend, by
//      specializing `usub::pg::json::custom_codec<T>`:
//
//        namespace usub::pg::json {
//            template <>
//            struct custom_codec<MyType> {
//                static std::string dump(const MyType& v);
//                static std::expected<MyType, std::string>
//                parse(std::string_view sv, bool strict);
//            };
//        }
//      A per-type override always wins over the global backend.
//
// Switching the backend FROM CODE (not CMake): the macros above
// (UPQ_JSON_BACKEND_GLAZE / _NLOHMANN / _CUSTOM) only need to be defined before
// any UPQ header is seen. The robust, ODR-safe way to do that without touching
// the build system is a config header named "upq_json_config.h" placed anywhere
// on your include path: UPQ auto-includes it (identically in every translation
// unit) before resolving the backend, so put your #defines and/or custom
// backend type there. Set UPQ_JSON_CONFIG_HEADER to a path of your choosing if
// you prefer a different name/location. CMake's -DUPQ_JSON_BACKEND=... is just a
// convenience that sets the same macros.
// =============================================================================

#include <expected>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

// ---- from-code configuration (no CMake / -D required) -----------------------
// Auto-include a user config header so the backend can be selected purely from
// code. It is discovered identically in every translation unit (before the
// backend is resolved), which keeps the choice ODR-consistent. Put your
// #define UPQ_JSON_BACKEND_* / custom backend type in there.
#if defined(UPQ_JSON_CONFIG_HEADER)
#include UPQ_JSON_CONFIG_HEADER
#elif __has_include(<upq_json_config.h>)
#include <upq_json_config.h>
#endif

// ---- backend selection ------------------------------------------------------
// Resolve exactly one backend id. Default = ujson (existing behavior).
//   0 = user-supplied custom, 1 = ujson, 2 = glaze, 3 = nlohmann/json
#if defined(UPQ_JSON_BACKEND_CUSTOM)
#define UPQ_JSON_BACKEND_ID 0
#elif defined(UPQ_JSON_BACKEND_GLAZE)
#define UPQ_JSON_BACKEND_ID 2
#elif defined(UPQ_JSON_BACKEND_NLOHMANN)
#define UPQ_JSON_BACKEND_ID 3
#else
#ifndef UPQ_JSON_BACKEND_UJSON
#define UPQ_JSON_BACKEND_UJSON 1
#endif
#define UPQ_JSON_BACKEND_ID 1
#endif

#if UPQ_JSON_BACKEND_ID == 1
#include <ujson/ujson.h>
#elif UPQ_JSON_BACKEND_ID == 2
#if !__has_include(<glaze/glaze.hpp>)
#error "UPQ_JSON_BACKEND_GLAZE selected but <glaze/glaze.hpp> is not on the include path. Install glaze and add it to your include dirs (e.g. find_package(glaze) + glaze::glaze)."
#endif
#include <glaze/glaze.hpp>
#elif UPQ_JSON_BACKEND_ID == 3
#if !__has_include(<nlohmann/json.hpp>)
#error "UPQ_JSON_BACKEND_NLOHMANN selected but <nlohmann/json.hpp> is not on the include path. Install nlohmann_json and add it to your include dirs (e.g. find_package(nlohmann_json) + nlohmann_json::nlohmann_json)."
#endif
#include <nlohmann/json.hpp>
#endif

namespace usub::pg::json {
    // -------------------------------------------------------------------------
    // Per-type override hook.
    //
    // Declared but intentionally left undefined: an unspecialized
    // `custom_codec<T>` is an incomplete type, which makes `has_custom_codec`
    // (below) SFINAE to false for every type the user has not opted in for.
    // -------------------------------------------------------------------------
    template <class T, class = void>
    struct custom_codec;

    namespace detail {
        template <class T, class = void>
        struct has_custom_codec : std::false_type {};

        template <class T>
        struct has_custom_codec<
            T, std::void_t<decltype(custom_codec<std::remove_cvref_t<T>>::dump(
                   std::declval<const std::remove_cvref_t<T> &>()))>> : std::true_type {};

        template <class T>
        inline constexpr bool has_custom_codec_v = has_custom_codec<T>::value;

        // ---- user-supplied custom backend -----------------------------------
        // UPQ_JSON_BACKEND_CUSTOM names a type exposing the same surface as the
        // built-in backends below: `name`, `dump<T>`, `parse<T, Strict>`.
#if UPQ_JSON_BACKEND_ID == 0
        using backend = UPQ_JSON_BACKEND_CUSTOM;

        // ---- ujson backend --------------------------------------------------
#elif UPQ_JSON_BACKEND_ID == 1
        struct backend {
            static constexpr std::string_view name = "ujson";

            template <class T>
            static std::string dump(const T &v) {
                return ::ujson::dump(v);
            }

            template <class T, bool Strict>
            static std::expected<T, std::string> parse(std::string_view sv) {
                auto r = ::ujson::try_parse<T, Strict>(sv);
                if (r) return std::move(*r);
                const auto &e = r.error();
                return std::unexpected(std::string("ujson parse failed: ") +
                                       (e.msg ? e.msg : "<null>"));
            }
        };

        // ---- glaze backend --------------------------------------------------
        // Uses glaze's compile-time aggregate reflection, so plain structs work
        // with no per-type code. `Strict` maps to glaze's `error_on_unknown_keys`
        // (which glaze also defaults to true).
#elif UPQ_JSON_BACKEND_ID == 2
        struct backend {
            static constexpr std::string_view name = "glaze";

            template <class T>
            static std::string dump(const T &v) {
                std::string out;
                // The buffer overload returns glz::error_ctx on recent glaze and
                // void on very old versions; (void)-cast keeps both compiling and
                // silences [[nodiscard]].
                (void) ::glz::write_json(v, out);
                return out;
            }

            template <class T, bool Strict>
            static std::expected<T, std::string> parse(std::string_view sv) {
                // glaze may over-read past the end with SIMD; feed it an owning,
                // resizable buffer rather than the raw view.
                std::string buf(sv);
                T v{};
                constexpr ::glz::opts opts{.error_on_unknown_keys = Strict};
                auto ec = ::glz::read<opts>(v, buf);
                if (ec) {
                    return std::unexpected(std::string("glaze parse failed: ") +
                                           ::glz::format_error(ec, buf));
                }
                return v;
            }
        };

        // ---- nlohmann/json backend ------------------------------------------
        // Follows nlohmann's own conventions: types are (de)serialized via
        // ADL `to_json`/`from_json` (e.g. NLOHMANN_DEFINE_TYPE_*). nlohmann has
        // no unknown-key rejection, so `Strict` is accepted but not enforced.
#elif UPQ_JSON_BACKEND_ID == 3
        struct backend {
            static constexpr std::string_view name = "nlohmann";

            template <class T>
            static std::string dump(const T &v) {
                return ::nlohmann::json(v).dump();
            }

            template <class T, bool Strict>
            static std::expected<T, std::string> parse(std::string_view sv) {
                try {
                    auto j = ::nlohmann::json::parse(sv.begin(), sv.end());
                    return j.template get<T>();
                } catch (const std::exception &e) {
                    return std::unexpected(std::string("nlohmann parse failed: ") + e.what());
                }
            }
        };
#endif
    }  // namespace detail

    // Human-readable name of the active default backend (diagnostics/tests).
    inline constexpr std::string_view backend_name = detail::backend::name;

    // -------------------------------------------------------------------------
    // Public entry points. A per-type `custom_codec<T>` override always wins;
    // otherwise the globally selected backend handles it.
    // -------------------------------------------------------------------------
    template <class T>
    [[nodiscard]] inline std::string dump(const T &v) {
        using U = std::remove_cvref_t<T>;
        if constexpr (detail::has_custom_codec_v<U>)
            return custom_codec<U>::dump(v);
        else
            return detail::backend::template dump<U>(v);
    }

    template <class T, bool Strict = true>
    [[nodiscard]] inline std::expected<std::remove_cvref_t<T>, std::string> parse(
        std::string_view sv) {
        using U = std::remove_cvref_t<T>;
        if constexpr (detail::has_custom_codec_v<U>)
            return custom_codec<U>::parse(sv, Strict);
        else
            return detail::backend::template parse<U, Strict>(sv);
    }
}  // namespace usub::pg::json

#endif  // UPQ_PG_JSON_CODEC_H
