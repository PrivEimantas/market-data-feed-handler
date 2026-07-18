# market-data-feed-handler






Known limitations (deliberate scope cuts for this POC):
- OrderReplace ('U') and Trade ('P') are parsed but not yet applied to the book
- Price precision: ITCH's 4-decimal scale is truncated to the book's cents-scale via integer division
- Latency benchmarking lives in the order-book project, not measured separately for the wire-to-book path