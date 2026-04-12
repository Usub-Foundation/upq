#include "upq/PgTypes.h"

#include <cstring>
#include <cstdlib>

namespace usub::pg
{
    void write_be32(uint8_t* dst, uint32_t v)
    {
        dst[0] = (v >> 24) & 0xFF;
        dst[1] = (v >> 16) & 0xFF;
        dst[2] = (v >> 8) & 0xFF;
        dst[3] = (v >> 0) & 0xFF;
    }

    uint32_t read_be32(const uint8_t* src)
    {
        return (uint32_t(src[0]) << 24) |
            (uint32_t(src[1]) << 16) |
            (uint32_t(src[2]) << 8) |
            (uint32_t(src[3]) << 0);
    }

    PgSqlStateClass classify_sqlstate(std::string_view sqlstate)
    {
        if (sqlstate.size() < 2)
            return PgSqlStateClass::Other;

        if (sqlstate == "23505") return PgSqlStateClass::UniqueViolation;
        if (sqlstate == "23514") return PgSqlStateClass::CheckViolation;
        if (sqlstate == "23502") return PgSqlStateClass::NotNullViolation;
        if (sqlstate == "23503") return PgSqlStateClass::ForeignKeyViolation;

        if (sqlstate == "40P01") return PgSqlStateClass::Deadlock;
        if (sqlstate == "40001") return PgSqlStateClass::SerializationFailure;

        if (sqlstate == "42501") return PgSqlStateClass::PrivilegeError;
        if (sqlstate == "42P01") return PgSqlStateClass::UndefinedObject;

        std::string_view cls = sqlstate.substr(0, 2);

        if (cls == "08") return PgSqlStateClass::ConnectionError; // connection exception
        if (cls == "22") return PgSqlStateClass::DataException; // data exception
        if (cls == "23") return PgSqlStateClass::ConstraintViolation; // integrity constraint violation
        if (cls == "25") return PgSqlStateClass::TransactionState; // invalid txn state
        if (cls == "28") return PgSqlStateClass::PrivilegeError; // invalid auth spec
        if (cls == "40") return PgSqlStateClass::TransactionState; // txn rollback
        if (cls == "42") return PgSqlStateClass::SyntaxError; // syntax/access rule violation
        if (cls == "XX") return PgSqlStateClass::InternalError; // internal error

        return PgSqlStateClass::Other;
    }

    PgServerErrorFields parse_error_fields(const std::vector<uint8_t>& payload)
    {
        PgServerErrorFields f;
        size_t i = 0;
        while (i < payload.size())
        {
            uint8_t code = payload[i++];
            if (code == 0) break;

            size_t end = i;
            while (end < payload.size() && payload[end] != 0) ++end;
            if (end >= payload.size()) {
                break;
            }
            const size_t len = end - i;

            std::string val(reinterpret_cast<const char*>(&payload[i]), len);

            switch (code)
            {
            case 'S': f.severity = std::move(val);
                break;
            case 'C': f.sqlstate = std::move(val);
                break;
            case 'M': f.message = std::move(val);
                break;
            case 'D': f.detail = std::move(val);
                break;
            case 'H': f.hint = std::move(val);
                break;
            default: break;
            }

            i = end + 1;
        }
        return f;
    }

    std::string parse_error(const std::vector<uint8_t>& payload)
    {
        auto f = parse_error_fields(payload);

        std::string msg;
        if (!f.message.empty()) msg += f.message;
        if (!f.detail.empty())
        {
            if (!msg.empty()) msg += " | detail: ";
            msg += f.detail;
        }
        if (!f.hint.empty())
        {
            if (!msg.empty()) msg += " | hint: ";
            msg += f.hint;
        }
        if (!f.sqlstate.empty())
        {
            if (!msg.empty()) msg += " | code: ";
            msg += f.sqlstate;
        }
        return msg;
    }

    void parse_row_description_ex(const std::vector<uint8_t>& payload,
                                  std::vector<std::string>& out_cols,
                                  RowParseContext& ctx)
    {
        ctx.ok = true;
        ctx.code = PgErrorCode::OK;
        ctx.msg.clear();

        out_cols.clear();

        auto rd16 = [&](size_t ofs) -> uint16_t
        {
            return (uint16_t(payload[ofs]) << 8) | uint16_t(payload[ofs + 1]);
        };

        size_t off = 0;
        if (payload.size() < 2)
        {
            ctx.ok = false;
            ctx.code = PgErrorCode::ParserTruncatedHeader;
            ctx.msg = "row_description too short (<2)";
            return;
        }

        uint16_t nfields = rd16(off);
        off += 2;

        out_cols.reserve(nfields);

        for (uint16_t i = 0; i < nfields; i++)
        {
            size_t start = off;
            while (off < payload.size() && payload[off] != 0) off++;

            if (off >= payload.size())
            {
                ctx.ok = false;
                ctx.code = PgErrorCode::ParserTruncatedHeader;
                ctx.msg = "column name not terminated";
                return;
            }

            std::string name(
                (const char*)&payload[start],
                off - start
            );
            off++;

            if (off + 18 > payload.size())
            {
                ctx.ok = false;
                ctx.code = PgErrorCode::ParserTruncatedHeader;
                ctx.msg = "row_description missing fixed tail (18 bytes)";
                return;
            }
            off += 18;

            out_cols.push_back(std::move(name));
        }
    }

    void parse_data_row_ex(const std::vector<uint8_t>& payload,
                           QueryResult::Row& out_row,
                           RowParseContext& ctx)
    {
        ctx.ok = true;
        ctx.code = PgErrorCode::OK;
        ctx.msg.clear();

        auto rd16 = [&](size_t ofs) -> uint16_t
        {
            return (uint16_t(payload[ofs]) << 8) | uint16_t(payload[ofs + 1]);
        };
        auto rd32 = [&](size_t ofs) -> uint32_t
        {
            return read_be32(&payload[ofs]);
        };

        size_t off = 0;
        if (payload.size() < 2)
        {
            ctx.ok = false;
            ctx.code = PgErrorCode::ParserTruncatedRow;
            ctx.msg = "data_row too short (<2)";
            return;
        }

        uint16_t ncols = rd16(off);
        off += 2;
        out_row.cols.reserve(ncols);

        for (uint16_t c = 0; c < ncols; c++)
        {
            if (off + 4 > payload.size())
            {
                ctx.ok = false;
                ctx.code = PgErrorCode::ParserTruncatedField;
                ctx.msg = "no space for field length";
                out_row.cols.emplace_back();
                return;
            }

            int32_t col_len = (int32_t)rd32(off);
            off += 4;

            if (col_len == -1)
            {
                out_row.cols.emplace_back();
                continue;
            }

            if (col_len < 0)
            {
                ctx.ok = false;
                ctx.code = PgErrorCode::ProtocolCorrupt;
                ctx.msg = "negative col_len";
                out_row.cols.emplace_back();
                return;
            }

            if (off + (size_t)col_len > payload.size())
            {
                ctx.ok = false;
                ctx.code = PgErrorCode::ParserTruncatedField;
                ctx.msg = "field overruns payload";
                out_row.cols.emplace_back();
                return;
            }

            out_row.cols.emplace_back(
                (const char*)&payload[off],
                (size_t)col_len
            );
            off += (size_t)col_len;
        }
    }

    std::vector<uint8_t> build_startup_message(const std::string& user,
                                               const std::string& db)
    {
        auto push_cstr = [&](std::vector<uint8_t>& v, const char* s)
        {
            while (*s) { v.push_back((uint8_t)*s++); }
            v.push_back(0);
        };
        auto push_kv = [&](std::vector<uint8_t>& v,
                           const char* k,
                           const std::string& val)
        {
            push_cstr(v, k);
            for (char c : val) v.push_back((uint8_t)c);
            v.push_back(0);
        };

        std::vector<uint8_t> tail;
        push_kv(tail, "user", user);
        push_kv(tail, "database", db);
        push_kv(tail, "client_encoding", "UTF8");
        tail.push_back(0); // terminator

        uint32_t total_len = 4 /*len*/ + 4 /*protocol*/ + (uint32_t)tail.size();

        std::vector<uint8_t> out;
        out.resize(8);
        write_be32(out.data() + 0, total_len);
        write_be32(out.data() + 4, 196608); // protocol 3.0
        out.insert(out.end(), tail.begin(), tail.end());

        return out;
    }

    std::vector<uint8_t> build_password_message(const std::string& password)
    {
        uint32_t len = 4 + (uint32_t)password.size() + 1;

        std::vector<uint8_t> out;
        out.reserve(1 + 4 + password.size() + 1);

        out.push_back('p');
        uint8_t be[4];
        write_be32(be, len);
        out.insert(out.end(), be, be + 4);
        out.insert(out.end(), password.begin(), password.end());
        out.push_back(0);

        return out;
    }

    std::string md5_hex(const uint8_t* data, size_t len)
    {
        uint8_t digest[MD5_DIGEST_LENGTH];
        MD5(data, len, digest);

        static const char* hex = "0123456789abcdef";
        std::string out;
        out.resize(32);
        for (int i = 0; i < 16; i++)
        {
            out[2 * i + 0] = hex[(digest[i] >> 4) & 0xF];
            out[2 * i + 1] = hex[(digest[i]) & 0xF];
        }
        return out;
    }

    std::vector<uint8_t> build_md5_password_message(
        const std::string& user,
        const std::string& password,
        const uint8_t salt[4]
    )
    {
        std::string step1_src = password + user;
        std::string step1_hex = md5_hex(
            reinterpret_cast<const uint8_t*>(step1_src.data()),
            step1_src.size()
        );

        std::string step2_src;
        step2_src.reserve(step1_hex.size() + 4);
        step2_src.append(step1_hex);
        step2_src.push_back((char)salt[0]);
        step2_src.push_back((char)salt[1]);
        step2_src.push_back((char)salt[2]);
        step2_src.push_back((char)salt[3]);

        std::string step2_hex = md5_hex(
            reinterpret_cast<const uint8_t*>(step2_src.data()),
            step2_src.size()
        );

        std::string final_str = "md5" + step2_hex;

        uint32_t msg_len = 4 + (uint32_t)final_str.size() + 1;
        std::vector<uint8_t> out;
        out.reserve(1 + 4 + final_str.size() + 1);

        out.push_back('p');
        uint8_t be[4];
        write_be32(be, msg_len);
        out.insert(out.end(), be, be + 4);
        out.insert(out.end(), final_str.begin(), final_str.end());
        out.push_back(0);

        return out;
    }

    void build_simple_query(std::vector<uint8_t>& out,
                            std::string_view sql)
    {
        uint32_t len = 4 + (uint32_t)sql.size() + 1;

        out.clear();
        out.reserve(1 + 4 + sql.size() + 1);

        out.push_back('Q');
        uint8_t be[4];
        write_be32(be, len);
        out.insert(out.end(), be, be + 4);
        out.insert(out.end(), sql.begin(), sql.end());
        out.push_back(0);
    }
} // namespace usub::pg