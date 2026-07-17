#pragma once
// NASDAQ TotalView-ITCH 5.0 - core message subset needed to build and
// maintain an order book. Deliberately excludes book-irrelevant types
// (Stock Directory, NOII, MWCB, IPO quoting period, etc.) - add them
// later if you want full spec coverage.
//
// All ITCH integers are big-endian on the wire. These structs are the
// *decoded* (host-endian) in-memory representation - parse_message()
// does the conversion, nothing here should be cast directly onto a
// wire buffer.

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>
#include <variant>

#include "byte_utils.hpp"

namespace feed::itch {

// Every message starts with this 11-byte header.
struct Header {
    char message_type;
    uint16_t stock_locate;
    uint16_t tracking_number;
    uint64_t timestamp_ns;  // nanoseconds since midnight
};

struct AddOrder {              // 'A' (36 bytes) / 'F' adds a 4-byte MPID we ignore for now
    Header header;
    uint64_t order_ref;
    char side;                 // 'B' or 'S'
    uint32_t shares;
    std::array<char, 8> stock; // space-padded ticker
    uint32_t price;            // 4 implied decimals: 1234500 == $123.45
};

struct OrderExecuted {         // 'E' (31 bytes)
    Header header;
    uint64_t order_ref;
    uint32_t executed_shares;
    uint64_t match_number;
};

struct OrderExecutedWithPrice { // 'C' (36 bytes)
    Header header;
    uint64_t order_ref;
    uint32_t executed_shares;
    uint64_t match_number;
    char printable;
    uint32_t execution_price;
};

struct OrderCancel {           // 'X' (23 bytes) - partial cancel
    Header header;
    uint64_t order_ref;
    uint32_t cancelled_shares;
};

struct OrderDelete {           // 'D' (19 bytes) - full removal
    Header header;
    uint64_t order_ref;
};

struct OrderReplace {          // 'U' (35 bytes) - cancel/replace, new ref number
    Header header;
    uint64_t original_order_ref;
    uint64_t new_order_ref;
    uint32_t shares;
    uint32_t price;
};

struct Trade {                 // 'P' (44 bytes) - non-cross execution against a hidden order
    Header header;
    uint64_t order_ref;
    char side;
    uint32_t shares;
    std::array<char, 8> stock;
    uint32_t price;
    uint64_t match_number;
};
// using a type-safe union which holds many types together at once, we define a type Message here which means it can be of any of these values
using Message = std::variant<AddOrder, OrderExecuted, OrderExecutedWithPrice,
                              OrderCancel, OrderDelete, OrderReplace, Trade>;

// Expected wire length for each type - used both to validate incoming
// messages and to size buffers when generating test traffic.
inline std::optional<size_t> expected_length(char message_type) {
    switch (message_type) {
        case 'A': return 36;
        case 'E': return 31;
        case 'C': return 36;
        case 'X': return 23;
        case 'D': return 19;
        case 'U': return 35;
        case 'P': return 44;
        default:  return std::nullopt; // unhandled type (F, S, R, H, ...) - caller should skip
    }
}

inline Header parse_header(const uint8_t* p) {
    Header h;
    h.message_type = static_cast<char>(p[0]);
    h.stock_locate = read_be16(p + 1);
    h.tracking_number = read_be16(p + 3);
    h.timestamp_ns = read_be48(p + 5);
    return h; // consumes bytes [0, 11)
}

inline std::array<char, 8> read_stock(const uint8_t* p) {
    std::array<char, 8> s;
    std::memcpy(s.data(), p, 8);
    return s;
}

// Parses one ITCH message. `len` must equal expected_length(type) for the
// type byte at data[0] - caller (the MoldUDP64 block iterator) guarantees
// this via the per-block length prefix.
inline std::optional<Message> parse_message(const uint8_t* data, size_t len) {
    if (len == 0) return std::nullopt;
    char type = static_cast<char>(data[0]);
    auto expected = expected_length(type);
    if (!expected || *expected != len) return std::nullopt;

    Header h = parse_header(data);
    const uint8_t* body = data + 11;

    switch (type) {
        case 'A': {
            AddOrder m{};
            m.header = h;
            m.order_ref = read_be64(body);
            m.side = static_cast<char>(body[8]);
            m.shares = read_be32(body + 9);
            m.stock = read_stock(body + 13);
            m.price = read_be32(body + 21);
            return m;
        }
        case 'E': {
            OrderExecuted m{};
            m.header = h;
            m.order_ref = read_be64(body);
            m.executed_shares = read_be32(body + 8);
            m.match_number = read_be64(body + 12);
            return m;
        }
        case 'C': {
            OrderExecutedWithPrice m{};
            m.header = h;
            m.order_ref = read_be64(body);
            m.executed_shares = read_be32(body + 8);
            m.match_number = read_be64(body + 12);
            m.printable = static_cast<char>(body[20]);
            m.execution_price = read_be32(body + 21);
            return m;
        }
        case 'X': {
            OrderCancel m{};
            m.header = h;
            m.order_ref = read_be64(body);
            m.cancelled_shares = read_be32(body + 8);
            return m;
        }
        case 'D': {
            OrderDelete m{};
            m.header = h;
            m.order_ref = read_be64(body);
            return m;
        }
        case 'U': {
            OrderReplace m{};
            m.header = h;
            m.original_order_ref = read_be64(body);
            m.new_order_ref = read_be64(body + 8);
            m.shares = read_be32(body + 16);
            m.price = read_be32(body + 20);
            return m;
        }
        case 'P': {
            Trade m{};
            m.header = h;
            m.order_ref = read_be64(body);
            m.side = static_cast<char>(body[8]);
            m.shares = read_be32(body + 9);
            m.stock = read_stock(body + 13);
            m.price = read_be32(body + 21);
            m.match_number = read_be64(body + 25);
            return m;
        }
        default:
            return std::nullopt;
    }
}

} // namespace feed::itch
