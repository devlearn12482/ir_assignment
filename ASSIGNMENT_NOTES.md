# Operations Tech Lead Assignment - Complete Notes

Source reviewed: `Operations Tech Lead - Assignment.pdf` (6 pages).

Working scope decision: the optional stretch tasks are not required for this submission. They are retained below only as a faithful record of the source document and should not be implemented unless the user later changes scope. The baseline will support multiple configured symbols on one combined WebSocket connection (`shard_id=0`), with independent per-symbol books and outputs. Distributing symbols across multiple WebSocket connections (multi-connection sharding) remains optional stretch work.

Working safety limit: accept at most 32 configured symbols on the baseline connection (96 subscribed streams) and reject a constructed WebSocket target longer than 8192 bytes. Fail fast with a clear configuration error instead of silently dropping or partially subscribing symbols.

## 1. Assignment objective and scope

- Build a single coherent solution in C++ only. It may be entirely original code or an extension of a provided codebase.
- Recommended language level: C++17 or newer.
- The README must explicitly state the compiler and C++ standard.
- The program must connect to Binance public WebSocket market-data streams.
- For every configured symbol, it must capture all three required traffic types:
  - Differential depth: `depth@100ms`
  - Partial top-five depth: `depth5@100ms`
  - Individual trades: `trade`
- It must write a market-data audit CSV with one row per inbound event applied by the process.
- It must maintain a local order book (LOB) in chronological processing order.
- It must emit order-book snapshot rows using the exact required schema.

## 2. Authoritative Binance references and connection details

- The official Binance documentation is authoritative for stream semantics and URL patterns.
- The document names these references:
  - [Spot WebSocket market streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams), including individual and combined streams.
  - [Spot combined-stream URL shape](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#combined-streams).
  - [USD-M Futures WebSocket market streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams).
- Example combined-stream base URLs:
  - Spot: `wss://stream.binance.com:9443/stream?streams=`
  - USD-M: `wss://fstream.binance.com/public/stream?streams=`
- The document explicitly notes that raw `@trade` for USD-M is on the `/public` path and directs the candidate to the Binance derivatives documentation.
- Current official USD-M documentation defines routed `/public`, `/market`, and `/private` endpoints and shows high-frequency depth streams on `/public/stream`. The PDF's `/public` form is therefore consistent with the current official documentation; the older unrouted form remains backward-compatible only for streams classified as Public.
- Keep venue endpoint and routed-path selection centralized and overridable for controlled testing; do not scatter URL literals through networking code. The README should state the production endpoints used and the date/current documentation against which they were verified.
- Combined stream names must use the lowercase symbol in the URL.
- Required suffixes per symbol:

| Suffix | Role |
|---|---|
| `<symbol>@depth@100ms` | Differential depth events (`depthUpdate`) |
| `<symbol>@depth5@100ms` | Partial top-of-book snapshot, intended as a sanity check or refresh |
| `<symbol>@trade` | Individual trades |

- Spot example for one symbol:
  - `btcusdt@depth@100ms/btcusdt@depth5@100ms/btcusdt@trade`

## 3. Deliverable A - market-data audit CSV

### 3.1 Event and payload rules

- Emit one row per message that the process applies from the socket.
- Parse the combined-stream envelope first.
- Store the logical inner Binance `data` payload, not the outer combined-stream envelope, in `payload_json`.
- Rows must be in processing order, which must be the same order observed by the LOB consumer.

### 3.2 File naming

- Suggested per-symbol naming:
  - `market_data_<venue>_<SYMBOL>_<UTC-date>.csv`
- A single file per run with a `symbol` column is also acceptable for multi-symbol capture.
- Whichever layout is chosen must be documented.

### 3.3 CSV encoding and escaping

- UTF-8.
- RFC 4180-style comma-separated data.
- A field containing a comma, quote, or newline must be double-quoted.
- A literal `"` inside a quoted field must be escaped as `""`.
- The first row must be a header.
- Header column names and order are fixed.
- `payload_json` must be minified, remain on one line, and be correctly CSV-escaped.
- No silent truncation is acceptable.

### 3.4 Exact market-data header

```csv
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

### 3.5 Market-data columns

| Column | Required meaning |
|---|---|
| `recv_tsec` | `int64`; seconds component of receive time under the selected time policy |
| `recv_tnsec` | `int32`; nanosecond remainder in `[0, 999999999]` |
| `venue` | `spot` or `usdm` |
| `stream_kind` | `depth_diff`, `depth5`, or `trade`; numeric `0`, `1`, `2` is allowed only if documented |
| `shard_id` | Non-negative connection-shard index; use `0` for a single connection |
| `conn_epoch` | Non-negative integer incremented on every WebSocket reconnect |
| `conn_seq` | Monotonic within each `(shard_id, conn_epoch)` pair |
| `symbol` | Uppercase symbol, for example `BTCUSDT` |
| `payload_json` | Inner Binance `data` object as one-line minified JSON, correctly CSV-escaped |

### 3.6 Market-data time policy

- Pick exactly one policy and document it.
- Recommended policy: wall-clock time at which the process finished reading the message.
  - If represented as nanoseconds, split it into `recv_tsec` and `recv_tnsec`.
- Permitted alternative: Binance event time from a JSON field such as `E` or `T`.
  - If this alternative is used, document the exact JSON field and its units.

**Working implementation decision:** use receive wall-clock time captured once immediately after the complete WebSocket message is read. Persist that value in the market-data row and reuse it unchanged for the corresponding order-book row. This provides a uniform policy for every stream, including Spot `depth5`, whose inner payload has no Binance event timestamp.

## 4. Deliverable B - order-book snapshot CSV

### 4.1 Emission policy

- Produce an order-book CSV for every supported symbol.
- Emit one row after every event that changes or refreshes the modeled book state.
- Mandatory minimum:
  - Every applied `depthUpdate` event.
  - Every applied `depth5` event.
- Trade-triggered rows are optional.
- If trades are included, document whether and how they affect the book or merely annotate a row.

### 4.2 CSV rules

- Same UTF-8 and CSV quoting rules as Deliverable A.
- The first row must be the exact header below, on one line.
- Every data row must contain exactly 26 columns, meaning 25 delimiters when none require quoting.

### 4.3 Exact order-book header

```csv
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

### 4.4 Order-book columns

| Column(s) | Type | Required meaning |
|---|---:|---|
| `tsec` | `int64` | Integer seconds for the row timestamp |
| `tnsec` | `int32` | Nanosecond remainder in `[0, 999999999]`; `tsec * 1e9 + tnsec` is the event time in nanoseconds |
| `seqNo` | `uint64` | Monotonic application sequence; increment once per emitted row |
| `id` | `int32` | Stable numeric instrument identifier for the session; derivation must be documented |
| `type` | `char` | One ASCII letter representing the last applied event class; mapping must be documented |
| `side` | `char` | `B` for bid-side, `S` for ask-side, `N` for symmetric/not applicable |
| `bid0` through `bid4` | `int64` | Top five bid prices, best bid first, as scaled integers |
| `bid_size0` through `bid_size4` | `int64` | Quantities at the corresponding bid levels, using the documented quantity scale |
| `ask0` through `ask4` | `int64` | Top five ask prices, best ask first, as scaled integers |
| `ask_size0` through `ask_size4` | `int64` | Quantities at the corresponding ask levels, using the documented quantity scale |

### 4.5 Snapshot timestamp policy

- Recommended: use the same timestamp source as the corresponding market-data audit row, such as `recv_tsec`/`recv_tnsec`.
- A Binance timestamp is also allowed.
- The selected source must be clearly documented.
- Reproducibility links this policy to replay: replay must copy `tsec`/`tnsec` from the stored market-data row and must never generate a new wall-clock timestamp. Otherwise a fixed audit input cannot reproduce byte-identical order-book output.

**Working implementation decision:** `tsec` and `tnsec` are exactly the `recv_tsec` and `recv_tnsec` stored for the triggering audit event.

### 4.6 Mandatory deterministic integer scaling

- Binance decimal values arrive as strings.
- Define and document fixed price and quantity scales, for example `10^8`.
- Convert decimal strings deterministically to scaled integers.
- Do not use binary floating-point for the integer CSV fields.
- Reviewers expect no `double` in the hot path to CSV integers unless the implementation is explicitly justified and demonstrably lossless.

### 4.7 Reconnect and gap behavior

- Document how `conn_epoch` affects book state after a reconnect.
- Document how Binance sequence fields `U`, `u`, and `pu` affect the LOB.
- The selected behavior may reset, resynchronize, or discard updates until a snapshot, but must be explicit and consistent with the CSV output.

## 5. Correct market-data semantics required

- Differential depth and `depth5` have different semantics.
- Differential depth updates amend the maintained state.
- A quantity of zero in a depth diff removes that price level.
- `depth5` has replacement/refresh semantics for the partial top-of-book view; it must not be treated as an ordinary diff.
- `depth5` payloads are venue-specific and must not be parsed through one assumed schema:
  - Spot partial depth contains `lastUpdateId`, `bids`, and `asks`. It has no inner `e`, `E`, `T`, or `s`; derive symbol and stream kind from the combined-stream envelope name.
  - Current USD-M partial depth is depth-update-like and contains `e`, `E`, `T`, `s`, `U`, `u`, `pu`, `b`, and `a` (and may contain additional documented fields such as `ps` and `st`).
  - The parser must dispatch by venue and envelope stream kind, validate required fields for that shape, and tolerate documented additive fields.
- Trades do not automatically update a diff-based order book.
- Any trade integration into book state must be explicitly documented.
- Sequence and gap handling is venue-specific:
  - Spot diffs contain `U` and `u`, but no `pu`.
  - After a Spot partial snapshot/refresh with update ID `L`, discard a diff with `u <= L`; declare a gap if `U > L + 1`; accept the first applicable diff when `U <= L + 1 <= u`.
  - For subsequent Spot diffs, `U == previous_u + 1` is normal. Do not require strict equality as the only acceptance test: ignore fully stale ranges and accept an overlapping range that spans the next expected update ID; declare a gap only when the next expected ID is not covered.
  - USD-M diffs contain `U`, `u`, and `pu`; after initialization, require `pu == previous_u`. A mismatch invalidates the modeled book until the selected refresh/reset policy restores it.
  - On a new `conn_epoch`, invalidate each affected symbol's book and wait for its next valid `depth5` refresh before applying diffs under the baseline partial-book model.
- Top five levels must have stable ordering:
  - Bids: best/highest first.
  - Asks: best/lowest first.
- From fixed input, the resulting output must be reproducible.

## 6. Required submission contents

### 6.1 Source and build

- C++ source only; no alternative runtime language for the solution.
- CMake is requested.
- An alternative build system requires justification and a reproducible build for a common Linux toolchain.

### 6.2 README contents

The README must include all of the following:

- Compiler.
- C++ standard.
- Dependencies.
- Complete build commands.
- Exact CLI invocation that produces both CSV outputs.
- A `--duration <seconds>` CLI option compatible with the document's reviewer command, which performs a bounded 300-second capture.
- Symbol-list format.
- Price and quantity scaling policies.
- Timestamp policy.
- Reconnect and gap-handling summary.
- Brief threading and I/O design note.
- Venue-specific WebSocket control-frame, connection-lifetime, and reconnect behavior.
- A short measured performance section even though formal performance notes are listed as optional stretch work; include replay throughput, processing-latency percentiles, and a memory/allocation measure with hardware, compiler flags, dataset size, and method.
- Chosen market-data file layout/naming if supporting multiple symbols.
- Instrument-ID derivation.
- `type` character mapping.
- Trade handling and whether trade events produce or annotate order-book rows.
- Replay behavior, including that grading replay makes no network calls if replay mode is provided.

### 6.3 Sample artifacts

- Include a short sample run, suggested duration 1-2 minutes with one symbol.
- Supply a `market_data_*.csv` file, either attached if small or truncated in the README.
- Supply the matching `*_orderbook.csv` covering the same time window.

### 6.4 Replay constraint

- Replay mode is strongly useful but not stated as a strict acceptance requirement.
- If provided, it must read the program's own `market_data_*.csv`, reproduce the order book, and make no network calls during grading replay.
- This offline behavior must be stated in the README.

## 7. Functional acceptance checklist

1. Live capture from Binance combined streams includes `depth@100ms`, `depth5@100ms`, and `trade`.
2. The market-data dump matches the exact column contract and row-per-event processing order.
3. The order-book CSV header matches exactly and every row has 26 data columns.
4. Top-five bid and ask values implement correct diff semantics; quantity `0` removes a level.
5. Integer scaling and timestamps are documented, and output is reproducible from fixed input.
6. Reconnect, sequence, and epoch behavior is stated.

## 8. Review focus - correctness and market-data literacy

- Correct separation of differential-depth semantics from `depth5` replacement semantics.
- Correct treatment of the trade stream.
- Explicit `U`/`u`/`pu` sequence and gap policy.
- Consistent new-epoch behavior after reconnect.
- Deterministic scaled-integer price/quantity conversion.
- Stable best-first bid/ask ordering.
- Correct RFC 4180 escaping of `payload_json`.
- Exact CSV headers.
- No silent truncation.
- Evidence must be present in code, build flags, and the README, not merely claimed.

## 9. Review focus - C++ quality and maintainability

- Clear RAII ownership and lifetimes.
- No dangling references across asynchronous callbacks.
- Clean WebSocket and file-I/O teardown on `SIGINT` and `SIGTERM`.
- Shutdown must be leak-free and friendly to Valgrind and sanitizers.
- Reconnect errors, partial reads, and parse failures must be logged or counted.
- No silent undefined behavior.
- Prefer appropriate modern C++ facilities such as `constexpr`, `enum class`, and scoped types.
- Avoid unnecessary macros.
- Preserve const correctness on hot structures.
- Use `-Wall -Wextra` or stricter warnings.
- Document a Release build.
- Leave no warnings unexplained.

## 10. Review focus - performance and scalability

Reviewers will specifically probe CPU consumption and allocation behavior.

- JSON parsing:
  - Choose an efficient parser, for example a SIMD-oriented parser, or document the fallback.
  - Avoid parsing the same payload more than once without need.
  - Reuse buffers where safe.
- Allocation behavior:
  - Minimize `std::string` churn and per-message heap allocations in the hot path.
  - Correct use of pooling, `reserve`, ring buffers, or small-buffer-optimization-friendly patterns is valued.
- Order-book update cost:
  - Top-five maintenance should be `O(updates)` per message with an appropriate data structure.
  - Explain the trade-off between the selected structure and a full-map order book.
- I/O:
  - Consider file and socket back-pressure.
  - Buffered writes or a dedicated writer thread are possible alternatives to blocking the network thread.
  - Document the chosen model and rationale.
- Threading:
  - If multi-threaded, define clear ownership and synchronization boundaries.
  - Ensure there are no data races.
  - Avoid a contended lock on every tick if claiming high throughput.

## 11. Review focus - observability and reviewability

- Provide counters for at least:
  - Messages processed.
  - Parse errors.
  - Reconnects.
  - Rows written.
- Replay from saved audit CSV to regenerated order-book CSV makes review and regression testing easier.

## 12. Security and operations baseline

- No secrets in the repository.
- Do not disable TLS verification without a strong justification.
- Enforce reasonable limits, including maximum URL size and/or shard size, to prevent runaway memory use from bad configuration.
- Configuration should use environment variables or a `.env.example` containing no real values.
- Never commit API keys, private keys, credentials, or secret configuration files.
- Treat WebSocket control frames as mainline protocol behavior:
  - Spot sends ping frames approximately every 20 seconds and requires a timely pong carrying the received payload.
  - Current USD-M documentation states a ping every 3 minutes and disconnection if no pong is received within 10 minutes.
  - The networking layer must process control frames while data and file I/O are busy; pong handling must not wait behind a blocked CSV write.
- Spot and USD-M connections have a hard 24-hour lifetime. Planned server disconnect, transport failure, and the bounded-capture timer must all enter the same tested asynchronous shutdown/reconnect state machine.
- A reconnect is normal lifecycle behavior, not an exceptional corner case: increment `conn_epoch`, restart `conn_seq`, count/log the reason, invalidate affected partial books, and resume only under the documented refresh policy.

## 13. Optional stretch work

**Current submission scope: not necessary / out of scope.**

- Obtain a REST depth snapshot and buffer/reconcile differential updates according to Binance documentation for proper resynchronization.
- Support multiple symbols with correct connection sharding.
- Include performance notes covering throughput and latency.

**Deliberate documentation exception:** multi-connection sharding and REST synchronization remain out of scope, but the final README will still include a short, reproducible set of measured performance results because performance is an explicit review priority. This does not expand the runtime feature scope.

## 14. GitHub submission section required at the end of README

The document explicitly asks for a GitHub submission section at the end of the README.

### 14.1 Initialize and push

```bash
# From the project root
git init
git add .
git commit -m "Initial submission: Binance WebSocket capture + LOB"

# Create a new GitHub repository through the web UI or CLI, then:
git remote add origin https://github.com/<your-username>/binance-lob-capture.git
git branch -M main
git push -u origin main
```

### 14.2 Required `.gitignore` examples

```gitignore
build/
*.csv
*.o
*.a
*.so
.env
config/secrets.json
```

- Note the tension between ignoring `*.csv` and submitting sample CSV artifacts: the submission must still provide the requested sample output, for example through an explicitly forced/curated sample, an attachment, or a README excerpt.

### 14.3 Tag the submission

```bash
git tag -a v1.0.0 -m "Submission v1.0.0"
git push origin v1.0.0
```

## 15. Reproducible environment and commands shown by the assignment

### 15.1 Prerequisites

Target environment shown: Ubuntu 22.04 / Debian.

```bash
sudo apt-get update
sudo apt-get install -y \
  cmake ninja-build \
  g++-12 \
  libssl-dev \
  libboost-all-dev \
  zlib1g-dev
```

- Compiler stated in the example: GCC 12 (`g++-12`).
- Standard stated in the example: C++17.
- The example says it was also tested with Clang 15.

### 15.2 Clone and build

```bash
git clone https://github.com/<your-username>/binance-lob-capture.git
cd binance-lob-capture

cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=g++-12 \
  -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"

cmake --build build --parallel
```

### 15.3 Live capture examples

Single-symbol spot capture:

```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT \
  --output-dir ./output
```

The document labels this as a two-minute capture but does not include a duration argument in this particular example. Its reviewer checklist later invokes `--duration 300`; therefore, the implementation must support bounded capture through `--duration <seconds>`. A two-minute invocation would add `--duration 120`.

Multi-symbol spot capture:

```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT,ETHUSDT \
  --output-dir ./output
```

USD-M Futures capture:

```bash
./build/binance_capture \
  --venue usdm \
  --symbols BTCUSDT \
  --output-dir ./output
```

### 15.4 Example output names

```text
output/
  market_data_spot_BTCUSDT_2025-01-15.csv
  market_data_spot_BTCUSDT_2025-01-15_orderbook.csv
```

### 15.5 Schema verification shown in the assignment

```bash
head -2 ./output/market_data_spot_BTCUSDT_2025-01-15.csv
head -2 ./output/market_data_spot_BTCUSDT_2025-01-15_orderbook.csv

awk -F',' 'NR==2{print NF}' ./output/market_data_spot_BTCUSDT_2025-01-15_orderbook.csv
# Expected output: 26
```

### 15.6 Test commands shown

```bash
cmake --build build --target tests
./build/tests/unit_tests
```

The document labels tests as "if applicable," although a senior-quality submission should treat focused correctness tests as expected review evidence.

## 16. Reviewer command checklist

| Review step | Command/result expected |
|---|---|
| Clean build | `cmake -B build && cmake --build build` |
| 300-second live capture | `./build/binance_capture --venue spot --symbols BTCUSDT --duration 300` |
| Order-book column count | `awk -F',' 'NR==2{print NF}' *_orderbook.csv` returns `26` |
| Secret scan | `git grep -i "api_key\|secret\|password"` returns empty |

## 17. Points that need an explicit implementation decision

The assignment allows choices in these areas, but every choice must be made consistently and documented:

- C++ standard newer than C++17, if selected.
- Capture duration and shutdown behavior for `--duration <seconds>`.
- Market-data file organization: one file per symbol/date or one multi-symbol file per run.
- Receive wall-clock versus a named Binance event timestamp.
- Snapshot-row timestamp source.
- Fixed price scale and fixed quantity scale.
- Instrument-ID derivation.
- `type` character mapping.
- `side` assignment for multi-side events and refreshes.
- Combined-stream routing: derive venue-specific symbol and stream kind from the envelope stream name before parsing the inner payload.
- `conn_seq` initialization and consumption policy.
- `seqNo` scope and initialization.
- Exact order-book filename derivation.
- Whether trade events produce order-book rows and whether they alter or only annotate book state.
- Reset, resync, or drop-until-snapshot behavior after reconnect or a sequence gap.
- Single-threaded versus multi-threaded pipeline.
- Blocking/buffered file I/O versus a dedicated writer.
- Order-book data structure and its trade-offs versus a full price-level map.
- Per-connection symbol/URL limits and behavior when exceeded. Under the current scope, up to 32 symbols share the single baseline connection (`shard_id=0`); multi-connection sharding is out of scope.

Working sequence and naming decisions:

- `conn_seq` starts at `1` for every `(shard_id, conn_epoch)` and is assigned immediately after each complete inbound WebSocket message is read, before JSON parsing. A parse failure therefore consumes a value and may create a visible sequence gap among applied audit rows; parse-error metrics explain the gap.
- `seqNo` starts at `1` independently for each symbol's order-book file and increments once per emitted row.
- Use per-symbol market-data and order-book files even when symbols share a connection. This avoids ambiguity because the exact order-book schema has no symbol column.
- Derive the order-book filename as `<market-data-file-stem>_orderbook.csv`, matching the assignment example.

## 18. Important assignment nuances and internal tensions

- The opening description says a snapshot is emitted after each processed event, while Deliverable B narrows the mandatory minimum to each applied `depthUpdate` and `depth5`; trade rows are explicitly optional. The implementation and README should follow a single documented policy.
- The PDF's displayed order-book header is visually clipped at the right page edge, but the subsequent column table and the 26-column acceptance check establish the full header recorded above.
- The suggested `.gitignore` excludes all `*.csv`, while the deliverables request sample CSVs. The candidate must deliberately include the required sample another way rather than accidentally omitting it.
- `depth5` is described both as a sanity check/refresh and as a mandatory book-refresh event. It is only a five-level partial snapshot: it may replace or seed the modeled visible top-five state, but it cannot reconstruct or resynchronize a full-depth Binance order book. With REST snapshot synchronization out of scope, the implementation must describe the maintained state as a partial/top-five model and must not claim full-book correctness.
- Full REST snapshot plus buffered-diff synchronization is listed as optional stretch work, but reviewers still expect a documented and consistent gap/reconnect policy in the base submission.
- Multi-symbol input and multi-connection sharding are separate concerns. The baseline accepts a comma-separated symbol list and routes all required streams over one combined connection; only distribution across two or more connections is excluded as stretch work.
- The official endpoint and payload documentation can change independently of the assignment PDF. Production URL routing and venue-specific schemas must be checked against official documentation during implementation, recorded in the README, and covered by parser/URL-construction tests.
