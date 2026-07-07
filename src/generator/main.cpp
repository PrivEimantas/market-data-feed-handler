// Synthetic exchange feed. Sends MoldUDP64-framed ITCH Add Order / Order
// Delete traffic over UDP to 127.0.0.1:<port>. Occasionally skips a
// sequence number on purpose so the receiver has real gaps to detect.
//
// This intentionally only emits 'A' and 'D' - phase 1 per the build plan.
// Add 'E'/'C'/'X'/'U'/'P' generation once the A/D path works end to end.

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <ctime>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "byte_utils.hpp"
#include "moldudp64.hpp"

using namespace feed;


// Anonymous namespace operates at the linking stage, after compilation. 
// Each .cpp file gets compiled independently into an object file (.o) with a symbol table - 
// a list of names it defines and names it needs from elsewhere. 
// The linker's job is to stitch multiple .o files together and make sure every name resolves to exactly one definition
// . Anonymous namespace tells the compiler: "give this symbol internal linkage" - 
// meaning don't even export this name into the object file's symbol table for other .o files to see. 
// It's invisible outside the file, not "visible but forbidden."


namespace {

// C++17-compatible version of "nanoseconds since midnight" - std::chrono::days
// and floor<days> are C++20 additions, so midnight is computed via <ctime> instead.
uint64_t now_ns_since_midnight() {
    using namespace std::chrono;
    auto now = system_clock::now();
    std::time_t tt = system_clock::to_time_t(now);
    std::tm tm_val{};
    localtime_r(&tt, &tm_val);
    tm_val.tm_hour = 0;
    tm_val.tm_min = 0;
    tm_val.tm_sec = 0;
    std::time_t midnight = std::mktime(&tm_val);

    auto midnight_tp = system_clock::from_time_t(midnight);
    return duration_cast<nanoseconds>(now - midnight_tp).count();
}

// Serializes one Add Order message body (without the 2-byte block length
// prefix - the caller adds that). Returns the 36-byte wire representation.


// we need const char* because strings are literal and so you pass a memory ref to first value (char)
// const just means we promise not to modify it (others can then see when looking at function)
// lets you pass in string literals (which live in ROM)


//** Pretends to be an exchange in real life, NASDAQ matching engine makes a
// Add Order ITCH message every time a new limit order enters the book.
// E.G: stock_locate=1, order_ref=<counter>, side='B'/'S', shares=100, ticker="TEST", price=<jittered>
// then  turns into exact 36 raw byte NASDAQ's wire format specifies, then handled to make_mold_packet
// then send over UDP
std::vector<uint8_t> make_add_order(uint16_t stock_locate, uint64_t order_ref,
                                     char side, uint32_t shares,
                                     const char* stock, uint32_t price) {
    std::vector<uint8_t> buf(36, 0); //sets all 36 vector elements to 0
    buf[0] = 'A';
    // Stock locate is passed as 16 bit number but buf only stores 8 bit chunks
    write_be16(&buf[1], stock_locate); // ticker represented as an int, its a mapping to an instrument
    write_be16(&buf[3], 0);                 // tracking number - unused here
    write_be48(&buf[5], now_ns_since_midnight());
    write_be64(&buf[11], order_ref);
    buf[19] = static_cast<uint8_t>(side);
    write_be32(&buf[20], shares);
    std::memset(&buf[24], ' ', 8);
    std::memcpy(&buf[24], stock, std::min<size_t>(8, std::strlen(stock)));
    write_be32(&buf[32], price);
    return buf;
}

std::vector<uint8_t> make_order_delete(uint16_t stock_locate, uint64_t order_ref) {
    std::vector<uint8_t> buf(19, 0);
    buf[0] = 'D';
    write_be16(&buf[1], stock_locate);
    write_be16(&buf[3], 0);
    write_be48(&buf[5], now_ns_since_midnight());
    write_be64(&buf[11], order_ref);
    return buf;
}

// Wraps a list of already-serialized ITCH messages into one MoldUDP64 packet.
std::vector<uint8_t> make_mold_packet(const std::string& session, uint64_t first_seq,
                                       const std::vector<std::vector<uint8_t>>& messages) {
    std::vector<uint8_t> packet(mold::kHeaderSize, 0);
    std::memset(packet.data(), ' ', 10);
    std::memcpy(packet.data(), session.data(), std::min<size_t>(10, session.size()));
    write_be64(&packet[10], first_seq);
    write_be16(&packet[18], static_cast<uint16_t>(messages.size()));

    for (const auto& msg : messages) {
        size_t off = packet.size();
        packet.resize(off + 2 + msg.size());
        write_be16(&packet[off], static_cast<uint16_t>(msg.size()));
        std::memcpy(&packet[off + 2], msg.data(), msg.size());
    }
    return packet;
}

} // namespace

int main(int argc, char** argv) {
    uint16_t port = 30001;
    if (argc > 1) port = static_cast<uint16_t>(std::stoi(argv[1]));

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { std::perror("socket"); return 1; }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr);

    std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> drop_roll(0, 19); // ~5% of packets get a gap injected
    std::uniform_int_distribution<int> price_jitter(-50, 50);

    const std::string session = "SESSION001";
    uint64_t seq = 1;
    uint64_t order_ref = 1;
    uint32_t base_price = 1000000; // $100.0000

    std::cout << "generator: sending to 127.0.0.1:" << port
              << " session=" << session << "\n";

    for (int i = 0; i < 2000; ++i) {
        std::vector<std::vector<uint8_t>> batch;

        char side = (i % 2 == 0) ? 'B' : 'S';
        uint32_t price = base_price + static_cast<uint32_t>(price_jitter(rng));
        batch.push_back(make_add_order(1, order_ref, side, 100, "TEST", price));

        // Every few orders, delete an earlier one to exercise removal.
        if (i > 5 && i % 4 == 0) {
            batch.push_back(make_order_delete(1, order_ref - 5));
        }

        auto packet = make_mold_packet(session, seq, batch);

        if (drop_roll(rng) == 0) {
            // Simulate loss: advance the sequence space without sending
            // this packet, so the receiver sees a real gap.
            std::cout << "generator: [simulated drop] seq " << seq
                      << " (" << batch.size() << " msgs)\n";
        } else {
            sendto(sock, packet.data(), packet.size(), 0,
                   reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
        }

        seq += batch.size();
        ++order_ref;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    close(sock);
    return 0;
}
