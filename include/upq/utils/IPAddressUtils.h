//
// Created by kirill on 1/24/26.
//

#ifndef UPQ_IPADDRESSUTILS_H
#define UPQ_IPADDRESSUTILS_H

#include <expected>
#include <optional>
#include <string>
#include <string_view>

#ifdef _WIN32
#  include <winsock2.h>
#  include <ws2tcpip.h>
#else
#  include <arpa/inet.h>
#endif

namespace usub::pg::utils {
    enum class ConninfoError {
        ContainsNul,
    };

    static inline std::string_view strip_brackets(std::string_view h) {
        if (h.size() >= 2 && h.front() == '[' && h.back() == ']')
            return std::string_view{h.data() + 1, h.size() - 2};
        return h;
    }

    static inline bool is_ip_literal(std::string_view host) {
        host = strip_brackets(host);

        if (host.find('%') != std::string_view::npos)
            return false;

        std::string host_cstr(host);

        unsigned char buf[sizeof(in6_addr)]{};
        return ::inet_pton(AF_INET, host_cstr.c_str(), buf) == 1 ||
               ::inet_pton(AF_INET6, host_cstr.c_str(), buf) == 1;
    }

    static inline std::expected<std::string, ConninfoError>
    escape_conninfo_value(std::string_view v) {
        if (v.find('\0') != std::string_view::npos)
            return std::unexpected(ConninfoError::ContainsNul);

        std::string out;
        out.reserve(v.size() + 2 + 8);
        out.push_back('\'');

        for (char c: v) {
            if (c == '\\' || c == '\'')
                out.push_back('\\');
            out.push_back(c);
        }

        out.push_back('\'');
        return out;
    }
}

#endif //UPQ_IPADDRESSUTILS_H