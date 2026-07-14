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
    std::vector<uint8_t> buf(36, 0); //sets all 36 vector elements to 0, this is speciifclaly for NASDAQ ITCH
    buf[0] = 'A';
    // Stock locate is passed as 16 bit number but buf only stores 8 bit chunks
    write_be16(&buf[1], stock_locate); // ticker represented as an int, its a mapping to an instrument
    write_be16(&buf[3], 0);                 // tracking number - unused here
    write_be48(&buf[5], now_ns_since_midnight());
    write_be64(&buf[11], order_ref);
    buf[19] = static_cast<uint8_t>(side);
    write_be32(&buf[20], shares);
    std::memset(&buf[24], ' ', 8); //writes same byte repeatedly 8 times here
    std::memcpy(&buf[24], stock, std::min<size_t>(8, std::strlen(stock))); // writes same byte repeatdly, count times starting at destination
    // we first add ' ' and then 'TEST' here instead of having random values in there
    write_be32(&buf[32], price);
    return buf;
}

// tells that an existing order has been fully removed from the book
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
// by searlized we mean converted to flat sequence of bytes to send over, rather than as  C++ object
// in this repo we make it be a vector of make_add_order messages

// we use & to access by reference here, avoids unneccessary allocation of memory plus copying over values (takes time + memory)
std::vector<uint8_t> make_mold_packet(const std::string& session, uint64_t first_seq,
                                       const std::vector<std::vector<uint8_t>>& messages) {

    //initialise 'packet' variable
    std::vector<uint8_t> packet(mold::kHeaderSize, 0); // go to mold namespace, get value of kHeaderSize and set size as 20, then populate values with 0
   
    std::memset(packet.data(), ' ', 10); // packet.data() = &packet[0]. we set the first 10 bytes here and line below
    std::memcpy(packet.data(), session.data(), std::min<size_t>(10, session.size())); 

    write_be64(&packet[10], first_seq); //8 bytes to write hence int64. its the first sequence because its from the first msg in packet, every subsequent msg in packet is derived from here.
    write_be16(&packet[18], static_cast<uint16_t>(messages.size())); // message count

    for (const auto& msg : messages) { //messages is just one 36 byte array at this point if no delete msg
        size_t off = packet.size(); // 20 here as we defined as header size
        packet.resize(off + 2 + msg.size()); // 20 + 2 + 36, so we redefine the vector to be bigger
        write_be16(&packet[off], static_cast<uint16_t>(msg.size())); ///write 2-byte length prefix at packet[20] and packet[21] encoded in big-endian 
        std::memcpy(&packet[off + 2], msg.data(), msg.size()); //write msg of msg.size
    }
    return packet;
}

} // namespace

// argc = argument count, argv is argument vector
int main(int argc, char** argv) {
    uint16_t port = 30001;
    if (argc > 1) port = static_cast<uint16_t>(std::stoi(argv[1])); //stoi = string to int. argv[1] just looks at string input as ASCII characters of all bytes for each integer
    // static cast = stoi returns plain int so we know its 4 bytes but could be +- 2 bil, so we cast for it to be in range of int16
    // so 999999 would wrap and truncate part of it after it got converted to an int
    // technically dont even need it as the compiler would figure it out, but would get a warning flagged and could create a silent bug that way
    // so we delibretly cast it so we tell compiler we know we are narrowing the values down

    int sock = socket(AF_INET, SOCK_DGRAM, 0); 
    //builtin. 0:= addr family:intrent (we want IPv4 like 127.0.0.1)
    // SOCK_DGRAM: socket type datagram , we say UDP not TCP which would be SOCK_STREAM
    // 0 protocol argument, picks default (UDP here for our arguments)
    if (sock < 0) { std::perror("socket"); return 1; }

    sockaddr_in dest{}; //socaddr_in is just a struct 
    dest.sin_family = AF_INET; //AF_INET is a constant and it specifies the IPv4 
    dest.sin_port = htons(port); // converts port from CPU native byte order to network byhte order (same logi here as write_be16) regardless of platform
    // port was just a regular unint16_t sat in host byte order

    inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr); //parses human-readable string of our localhost to into binary IP address format
    // its written  into sin_addr



    // faking some realistic, market data
    std::mt19937 rng{std::random_device{}()}; //mersenne twister, specific pseudorandom number generator
    // use to seet for non-detrminisitc number gen


    std::uniform_int_distribution<int> drop_roll(0, 19); // ~5% of packets get a gap injected
    // uses rng raw output to produce uniformly random int in the range


    std::uniform_int_distribution<int> price_jitter(-50, 50); 
    // using to make prices wobble around their values later on rather than looking too static

    const std::string session = "SESSION001";
    uint64_t seq = 1;
    uint64_t order_ref = 1;
    uint32_t base_price = 1000000; // $100.0000

    std::cout << "generator: sending to 127.0.0.1:" << port
              << " session=" << session << "\n";

    for (int i = 0; i < 2000; ++i) {
        std::vector<std::vector<uint8_t>> batch; //build up with 1-2 msgs to send via socket

        char side = (i % 2 == 0) ? 'B' : 'S';
        uint32_t price = base_price + static_cast<uint32_t>(price_jitter(rng)); //make price slightly fluctuate
        batch.push_back(make_add_order(1, order_ref, side, 100, "TEST", price));

        // Every few orders, delete an earlier one to exercise removal.
        if (i > 5 && i % 4 == 0) {
            batch.push_back(make_order_delete(1, order_ref - 5)); //add extra msg to delete, referencing order from 5 iteratiosns ago
        }

        auto packet = make_mold_packet(session, seq, batch);

        if (drop_roll(rng) == 0) { 
            // Simulate loss: advance the sequence space without sending
            // this packet, so the receiver sees a real gap.
            std::cout << "generator: [simulated drop] seq " << seq
                      << " (" << batch.size() << " msgs)\n";
        } else {
            //sends over socket
            // sock: which socket
            // packet.data : pointer to raw bytes to send (header + messages here)
            // packet.size : how many bytes to send
            // 0 : flags which we dont need
            // where to send: expects generic sockaddr* , we cast sockaddr_in* as sockaddr*
            // how big the address struct is we pass last
            sendto(sock, packet.data(), packet.size(), 0,
                   reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
        }

        seq += batch.size(); // we dont know if we send 1 or 2 messages via socket, so next seqence would need to start 2 higher if it was 2 msgs
        // we also need to know that a message was not received, aka if drop_roll runs
        
        ++order_ref;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    close(sock);
    return 0;
}
