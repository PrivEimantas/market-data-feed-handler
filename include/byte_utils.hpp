#pragma once
// Big-endian (network byte order) helpers.
// ITCH and MoldUDP64 specify all integers as unsigned big-endian, so every
// multi-byte field has to be read/written through here rather than cast
// directly off the wire buffer (x86 is little-endian).

#include <cstdint>
#include <cstring>

namespace feed {

inline uint16_t read_be16(const uint8_t* p) {
    return static_cast<uint16_t>((p[0] << 8) | p[1]);
}

inline uint32_t read_be32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) |
           (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8)  |
            static_cast<uint32_t>(p[3]);
}

inline uint64_t read_be64(const uint8_t* p) {
    uint64_t hi = read_be32(p);
    uint64_t lo = read_be32(p + 4);
    return (hi << 32) | lo;
}

// ITCH timestamps are only 6 bytes (48 bits) - nanoseconds since midnight.
inline uint64_t read_be48(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 6; ++i) v = (v << 8) | p[i];
    return v;
}

inline void write_be16(uint8_t* p, uint16_t v) {
    p[0] = static_cast<uint8_t>(v >> 8);
    p[1] = static_cast<uint8_t>(v);
}

inline void write_be32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v >> 24);
    p[1] = static_cast<uint8_t>(v >> 16);
    p[2] = static_cast<uint8_t>(v >> 8);
    p[3] = static_cast<uint8_t>(v);
}

inline void write_be64(uint8_t* p, uint64_t v) {
    write_be32(p, static_cast<uint32_t>(v >> 32));
    write_be32(p + 4, static_cast<uint32_t>(v));
}

inline void write_be48(uint8_t* p, uint64_t v) {
    for (int i = 5; i >= 0; --i) {
        p[i] = static_cast<uint8_t>(v & 0xFF);
        v >>= 8;
    }
}

} // namespace feed
