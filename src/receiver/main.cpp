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

#include "external/order-book/order-book/OrderBook.h"
#include "itch_messages.hpp"
#include "moldudp64.hpp"

using namespace feed;

namespace {


Side itch_side_to_side(char side) {
switch (side) {
    case 'B': return Side::Buy;
    case 'S': return Side::Sell;
    default:
        throw std::invalid_argument("unexpected side character in ITCH message");
    }
}

// Placeholder consumer. Swap this for ring_buffer.push(msg) once you wire
// this into the order book project.
void on_message(const itch::Message& msg, OrderBook& book) {
    std::visit([&book](const auto& m) {
        using T = std::decay_t<decltype(m)>;

        if constexpr (std::is_same_v<T, itch::AddOrder>) {
            Order orderToExecute{};
            orderToExecute.id = static_cast<int64_t>(m.order_ref);
            orderToExecute.side = itch_side_to_side(m.side);
            orderToExecute.price = static_cast<int64_t>(m.price);
            orderToExecute.quantity = static_cast<int64_t>(m.shares);
            orderToExecute.timestamp = std::chrono::high_resolution_clock::now();

            book.addOrder(orderToExecute);

        } else if constexpr (std::is_same_v<T, itch::OrderDelete>) {
            // 'D' - full removal of a resting order.
            book.cancelOrder(static_cast<int64_t>(m.order_ref));

        } else if constexpr (std::is_same_v<T, itch::OrderCancel>) {
            // 'X' - PARTIAL cancel: reduce remaining quantity, don't remove outright.
            book.reduceOrder(static_cast<int64_t>(m.order_ref),
                              static_cast<int64_t>(m.cancelled_shares));

        } else if constexpr (std::is_same_v<T, itch::OrderExecuted>) {
            book.reduceOrder(static_cast<int64_t>(m.order_ref),
                              static_cast<int64_t>(m.executed_shares));

        } else if constexpr (std::is_same_v<T, itch::OrderExecutedWithPrice>) {
            book.reduceOrder(static_cast<int64_t>(m.order_ref),
                              static_cast<int64_t>(m.executed_shares));

        } else {
            // OrderReplace ('U') and Trade ('P') aren't wired yet - known gap.
            std::cout << "  (unhandled message type)\n";
        }
    }, msg);
}

} //namespace

int main(int argc, char** argv) {
    uint16_t port = 30001;
    if (argc > 1) port = static_cast<uint16_t>(std::stoi(argv[1]));

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { std::perror("socket"); return 1; }

    OrderBook book; 

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { //if fails. follows POSIX convention
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
        ssize_t n = recv(sock, buf.data(), buf.size(), 0); // how many bytes arrived. cant use negatives as they can mean failure, so we use signed_size_t
        // the actual data goes into buf.data()  for buf.size()
        if (n < static_cast<ssize_t>(mold::kHeaderSize)) continue; // too small to be valid


        // turnign the raw incoming bytes to something useful, reads first 20 bytes and reconstructs session
        mold::Header header = mold::parse_header(buf.data()); //return raw ptr to first element (&buf[0])
        ++packets_seen;


        // generator not setting these anywhere for these to pass
        if (header.message_count == mold::kHeartbeat) continue; //  pinging, if packet is a heartbeat dont do anything just move on
        if (header.message_count == mold::kEndOfSession) {
            std::cout << "receiver: end-of-session marker, stopping\n";
            break;
        }

        // inspect after 20 byte header, reading each msg 2-byte length prefix then extract bytes as payload
        // basically if this follows out format we laid out
        if (!mold::parse_blocks(buf.data(), static_cast<size_t>(n), header, blocks)) {
            std::cout << "receiver: malformed packet, dropping\n";
            continue;
        }


        for (const auto& block : blocks) {
            int64_t gap = tracker.on_sequence(block.sequence_number);
            if (gap > 0) { // we missed a msg, aka a packet fell through
                gaps_detected += static_cast<uint64_t>(gap);
                std::cout << "receiver: GAP detected - missed " << gap
                          << " message(s) before seq " << block.sequence_number
                          << " (would trigger retransmit request here)\n";
            } else if (gap < 0) {
                std::cout << "receiver: duplicate/reordered seq "
                          << block.sequence_number << ", skipping\n";
                continue;
            }

            // turn the msg raw bytes into actual AddOrder etc
            auto parsed = itch::parse_message(block.data, block.length);
            if (!parsed) continue; // unhandled message type, e.g. non-book types
            ++messages_seen;
            on_message(*parsed,book); 
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


