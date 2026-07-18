# market-data-feed-handler
# market-data-feed-handler

A NASDAQ TotalView-ITCH 5.0 feed handler: a simulated exchange sends real
MoldUDP64-framed ITCH traffic over UDP; a receiver unwraps the transport
framing, detects sequence gaps, parses the wire format, and feeds the
resulting order events into a live order book.

This is the natural extension of [order-book](https://github.com/PrivEimantas/order-book).
Where that project proved out the matching engine and data structures in
isolation, this project builds the pipeline that would actually feed it in
a real system: **wire → transport framing → gap detection → parsing → book**.

## Why this exists

Order books are a solved data-structure problem once the invariants are
right. The interesting failure modes in real trading infrastructure live
one layer down, in the feed handler: out-of-order packets, dropped UDP
datagrams, binary protocol parsing, and the translation boundary between
an exchange's wire format and an internal book representation. This
project exists to prove that layer, not just the book itself.

## Architecture

```
generator (simulated exchange)
    │  builds ITCH messages (Add Order, Order Delete, ...)
    │  wraps them in MoldUDP64 framing
    │  sends over UDP, randomly dropping ~5% of packets
    ▼
receiver (the actual feed handler)
    │  recv() raw UDP packets
    │  unwrap MoldUDP64 header + message blocks
    │  track sequence numbers, detect & log gaps
    │  parse ITCH bytes into typed messages (std::variant)
    │  translate into the order book's domain (price scale, Side enum, ...)
    ▼
OrderBook (external, submoduled from PrivEimantas/order-book)
    addOrder() / cancelOrder() / reduceOrder()
```

### Components

- **`include/byte_utils.hpp`** - big-endian read/write helpers. Both ITCH
  and MoldUDP64 specify all integers as unsigned big-endian on the wire.
- **`include/moldudp64.hpp`** - the real transport framing NASDAQ wraps
  ITCH in. One UDP packet can contain multiple ITCH messages; sequence
  numbers increment per-message, which is what makes gap detection
  possible (`SequenceTracker`).
- **`include/itch_messages.hpp`** - struct definitions and a wire parser
  for the order-book-relevant ITCH subset: `A` (Add Order), `E` (Order
  Executed), `C` (Order Executed w/ Price), `X` (Order Cancel - partial),
  `D` (Order Delete - full removal), `U` (Order Replace), `P` (Trade).
- **`src/generator/main.cpp`** - a synthetic exchange. Builds Add/Delete
  traffic, wraps it in MoldUDP64 packets, sends over loopback UDP, and
  randomly skips ~5% of packets (while still advancing the sequence
  space) to produce real, detectable gaps on the receiving end.
- **`src/receiver/main.cpp`** - the feed handler itself. Receives,
  unwraps, gap-checks, parses, and hands each message to `on_message()`,
  which translates it into the order book's domain and calls the
  appropriate `OrderBook` method.
- **`external/order-book`** - git submodule, the matching engine this
  pipeline feeds.

## Build

```bash
g++ -std=c++17 -Wall -Wextra -O2 -Iinclude -I. \
    src/generator/main.cpp -o generator

g++ -std=c++17 -Wall -Wextra -O2 -Iinclude -I. \
    src/receiver/main.cpp external/order-book/order-book/OrderBook.cpp \
    -o receiver
```

If cloning fresh, pull the submodule first:
```bash
git submodule update --init --recursive
```

Requires a POSIX sockets environment (Linux or WSL2 - not native Windows;
`arpa/inet.h`/`sys/socket.h` have no Winsock equivalent used here).

## Run

```bash
./receiver 30001 &
./generator 30001
```

Expected output on the receiver side: periodic stats lines
(`packets=... messages=... gaps=...`) and occasional gap detections
corresponding to the generator's simulated packet loss.

## Verified behavior

This has been run and manually confirmed, not just compiled:
- Message counts and gap counts land at the rates the generator's own
  drop probability (~5%) predicts.
- Orders added via `AddOrder` appear in the book with correct side and
  quantity.
- Orders removed via `OrderDelete` correctly disappear from later book
  snapshots, roughly 5 iterations after being added (matching the
  generator's simulated lifecycle).
- Sequence gap detection fires only on genuinely dropped packets, not on
  in-order traffic.

## Known limitations (deliberate scope cuts)

- **`OrderReplace` ('U') and `Trade` ('P') are parsed but not yet applied
  to the book.** The generator doesn't currently emit either type, so
  this path is unexercised. `OrderReplace` in particular needs a new
  `OrderBook` method that retires one order ID and inserts a new one.
- **Price precision is truncated.** ITCH prices carry 4 implied decimal
  places; the order book stores prices as integer cents. The conversion
  (`price / 100`) is integer division, so ITCH's finer-grained jitter
  (used to simulate realistic price movement) gets silently rounded away
  below one cent. This is a known, understood truncation, not a bug.
- **Latency benchmarking lives in the order-book project**, measured
  around its own matching logic. This pipeline doesn't yet have a
  separate wire-to-book latency measurement, since `matchOrders` (the
  only place the original benchmarking hooked in) isn't on this feed's
  code path - ITCH messages arrive pre-matched from the (simulated)
  exchange, so book updates go through `addOrder`/`cancelOrder`/
  `reduceOrder` instead.
- **No retransmission.** Gaps are detected and logged
  (`would trigger retransmit request here`) but not recovered from. Real
  MoldUDP64 implementations request missed messages over a separate
  channel; this project stops at detection.
- **Synthetic traffic only.** The generator produces plausible but
  invented order flow. NASDAQ publishes free historical ITCH sample data
  specifically for testing parsers - replaying that would be a stronger
  claim than synthetic traffic, but hasn't been done here.

## Possible next steps

1. Wire `OrderReplace` and `Trade` into `on_message()`.
2. Replace synthetic generator traffic with a real NASDAQ ITCH sample file.
3. Implement MoldUDP64 retransmission requests on gap detection.
4. Add a dedicated wire-to-book latency measurement, independent of the
   order book's own internal benchmarking.