// Feed handler receiver. This is the network thread: its only job is to
// pull packets off the socket, unwrap MoldUDP64 framing, detect sequence
// gaps, and hand parsed ITCH messages to a consumer as fast as possible.
//
// Right now the "consumer" is a print statement. Replace on_message()'s
// body with a push onto your SPSC ring buffer from the order book project -
// that's the actual wire-to-book pipeline this project exists to build.

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <variant>
#include <vector>

#include "itch_messages.hpp"
#include "moldudp64.hpp"

using namespace feed;

namespace {

// Placeholder consumer. Swap this for ring_buffer.push(msg) once you wire
// this into the order book project.
void on_message(const itch::Message& msg) {
    std::visit([](const auto& m) {
        using T = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<T, itch::AddOrder>) {
            std::cout << "  ADD  ref=" << m.order_ref
                      << " side=" << m.side
                      << " shares=" << m.shares
                      << " price=" << (m.price / 10000.0) << "\n";
        } else if constexpr (std::is_same_v<T, itch::OrderDelete>) {
            std::cout << "  DEL  ref=" << m.order_ref << "\n";
        } else if constexpr (std::is_same_v<T, itch::OrderCancel>) {
            std::cout << "  CXL  ref=" << m.order_ref
                      << " shares=" << m.cancelled_shares << "\n";
        } else if constexpr (std::is_same_v<T, itch::OrderExecuted>) {
            std::cout << "  EXEC ref=" << m.order_ref
                      << " shares=" << m.executed_shares << "\n";
        } else {
            std::cout << "  (other message type)\n";
        }
    }, msg);
}

} // namespace

int main(int argc, char** argv) {
    uint16_t port = 30001;
    if (argc > 1) port = static_cast<uint16_t>(std::stoi(argv[1]));

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { std::perror("socket"); return 1; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        return 1;
    }

    std::cout << "receiver: listening on udp:" << port << "\n";

    mold::SequenceTracker tracker;
    std::vector<uint8_t> buf(65536);
    std::vector<mold::MessageBlock> blocks;

    uint64_t packets_seen = 0;
    uint64_t messages_seen = 0;
    uint64_t gaps_detected = 0;

    while (true) {
        ssize_t n = recv(sock, buf.data(), buf.size(), 0);
        if (n < static_cast<ssize_t>(mold::kHeaderSize)) continue; // too small to be valid

        mold::Header header = mold::parse_header(buf.data());
        ++packets_seen;

        if (header.message_count == mold::kHeartbeat) continue;
        if (header.message_count == mold::kEndOfSession) {
            std::cout << "receiver: end-of-session marker, stopping\n";
            break;
        }

        if (!mold::parse_blocks(buf.data(), static_cast<size_t>(n), header, blocks)) {
            std::cout << "receiver: malformed packet, dropping\n";
            continue;
        }

        for (const auto& block : blocks) {
            int64_t gap = tracker.on_sequence(block.sequence_number);
            if (gap > 0) {
                gaps_detected += static_cast<uint64_t>(gap);
                std::cout << "receiver: GAP detected - missed " << gap
                          << " message(s) before seq " << block.sequence_number
                          << " (would trigger retransmit request here)\n";
            } else if (gap < 0) {
                std::cout << "receiver: duplicate/reordered seq "
                          << block.sequence_number << ", skipping\n";
                continue;
            }

            auto parsed = itch::parse_message(block.data, block.length);
            if (!parsed) continue; // unhandled message type, e.g. non-book types
            ++messages_seen;
            on_message(*parsed);
        }

        if (packets_seen % 100 == 0) {
            std::cout << "-- stats: packets=" << packets_seen
                      << " messages=" << messages_seen
                      << " gaps=" << gaps_detected << " --\n";
        }
    }

    close(sock);
    return 0;
}
