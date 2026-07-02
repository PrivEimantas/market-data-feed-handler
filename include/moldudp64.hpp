#pragma once
// MoldUDP64 - the transport framing NASDAQ actually wraps ITCH in.
// One UDP packet = one MoldUDP64 header + N message blocks.
// Sequence numbers increment per-message (not per-packet), which is what
// makes gap detection possible: if a packet says "sequence 104, count 2"
// and the last packet you saw ended at sequence 103, you're caught up.
// If it says "sequence 106", you know you missed messages 104-105.

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "byte_utils.hpp"

namespace feed::mold {

constexpr size_t kHeaderSize = 20; // 10 (session) + 8 (seq num) + 2 (msg count)
constexpr uint16_t kHeartbeat = 0x0000;
constexpr uint16_t kEndOfSession = 0xFFFF;

struct Header {
    std::string session;      // 10 bytes, ASCII, space-padded
    uint64_t sequence_number; // sequence of the FIRST message block in this packet
    uint16_t message_count;   // may be 0 (heartbeat) or 0xFFFF (end of session)
};

inline Header parse_header(const uint8_t* p) {
    Header h;
    h.session.assign(reinterpret_cast<const char*>(p), 10);
    h.sequence_number = read_be64(p + 10);
    h.message_count = read_be16(p + 18);
    return h;
}

// A single (length, pointer) view into one message block's ITCH payload.
// Does not own the memory - only valid as long as the source packet buffer is.
struct MessageBlock {
    uint64_t sequence_number; // this block's own sequence (header.seq + index)
    const uint8_t* data;
    uint16_t length;
};

// Walks the message blocks in a MoldUDP64 packet after the 20-byte header.
// Each block is: 2-byte big-endian length prefix + that many bytes of ITCH message.
// Returns false (and stops early) if the packet is malformed / truncated -
// callers should treat that as a corrupt packet, not a sequence gap.
inline bool parse_blocks(const uint8_t* packet, size_t packet_len,
                          const Header& header, std::vector<MessageBlock>& out) {
    out.clear();
    size_t offset = kHeaderSize;
    uint64_t seq = header.sequence_number;

    for (uint16_t i = 0; i < header.message_count; ++i) {
        if (offset + 2 > packet_len) return false; // truncated length prefix
        uint16_t len = read_be16(packet + offset);
        offset += 2;
        if (offset + len > packet_len) return false; // truncated payload
        out.push_back(MessageBlock{seq, packet + offset, len});
        offset += len;
        ++seq;
    }
    return true;
}

// Tracks expected sequence number across packets and reports gaps.
// This is the whole gap-detection story for your feed handler - the
// interesting part isn't the arithmetic, it's what you do when a gap
// fires (log + wait for a reset, or request retransmission).
class SequenceTracker {
public:
    // Returns the number of missed messages (0 if none, i.e. in order).
    // Call once per *message block* you process, not once per packet.
    int64_t on_sequence(uint64_t seq) {
        if (!have_seen_any_) {
            have_seen_any_ = true;
            expected_ = seq + 1;
            return 0;
        }
        int64_t gap = static_cast<int64_t>(seq) - static_cast<int64_t>(expected_);
        expected_ = seq + 1;
        return gap; // >0 means we missed `gap` messages, <0 means duplicate/reorder
    }

    uint64_t expected() const { return expected_; }

private:
    bool have_seen_any_ = false;
    uint64_t expected_ = 0;
};

} // namespace feed::mold
