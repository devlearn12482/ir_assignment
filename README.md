# Binance Market-Data Capture and Partial Top-Five Book

This repository is a C++17 implementation of the assignment's mandatory
baseline. It captures Binance Spot or USD-M combined WebSocket streams,
writes a byte-faithful market-data audit CSV, maintains one deterministic
partial top-five book per configured symbol, and writes the required
26-column order-book CSV. The same order-book engine is available through an
offline replay mode.

The book is deliberately described as **partial top-five state**, not a full
Binance order book. `depth5` is used as an independent visible-depth refresh;
there is no REST snapshot or full-depth reconstruction. Multi-connection
sharding and a full end-to-end performance suite are optional stretch work
and are not implemented; one isolated core benchmark is retained as review
evidence.

## Reviewer quick start

The following is the shortest complete Ubuntu 22.04 review path. It uses the
submitted tag, the assignment's GCC 12 Release flags, the public test target,
a 300-second Spot capture, the required 26-column check, and deterministic
offline replay. No API key or other credential is required.

```bash
git checkout v1.0.0

sudo apt-get update
sudo apt-get install -y \
  cmake ninja-build \
  g++ g++-12 \
  libssl-dev \
  libboost-all-dev \
  zlib1g-dev

cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=g++-12 \
  -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"
cmake --build build --parallel
cmake --build build --target tests

rm -rf reviewer-output
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT \
  --duration 300 \
  --output-dir reviewer-output

audit_file="$(find reviewer-output -maxdepth 1 -type f \
  -name 'market_data_spot_BTCUSDT_*.csv' \
  ! -name '*_orderbook.csv' -print -quit)"
book_file="$(find reviewer-output -maxdepth 1 -type f \
  -name 'market_data_spot_BTCUSDT_*_orderbook.csv' -print -quit)"
head -1 "${audit_file}"
head -1 "${book_file}"
awk -F',' 'NR == 2 { print NF }' "${book_file}"

rm -rf reviewer-replay
./build/binance_capture \
  --replay testdata/replay/market_data_spot_BTCUSDT_fixture.csv \
  --output-dir reviewer-replay
cmp \
  testdata/replay/expected/market_data_spot_BTCUSDT_fixture_orderbook.csv \
  reviewer-replay/market_data_spot_BTCUSDT_fixture_orderbook.csv
```

Expected results: all seven registered test groups pass, live capture writes
one audit/book pair and exits with `run.status=success`, the `awk` command
prints `26`, and `cmp` produces no output and returns zero. The remainder of
this README documents the exact contracts, additional Spot/USD-M commands,
failure policies, metrics, and evidence behind those checks.

## Build environment

Reference environment:

- Ubuntu 22.04
- GCC 12 (`g++-12`); Ubuntu 22.04's default GCC 11 (`g++`) is also exercised
  by the clean reviewer build
- C++17 with compiler extensions disabled
- CMake 3.22 or newer and Ninja
- Boost 1.74 or newer (`Asio`, `Beast`, `System`)
- OpenSSL 3.0 or newer
- vendored simdjson 3.6.4

Install the assignment dependencies:

```bash
sudo apt-get update
sudo apt-get install -y \
  cmake ninja-build \
  g++ g++-12 \
  libssl-dev \
  libboost-all-dev \
  zlib1g-dev
```

The project does not link zlib. Beast's WebSocket type is instantiated
without deflate support, so `permessage-deflate` is never offered.

Build with the PDF's GCC 12 command:

```bash
cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=g++-12 \
  -DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"
cmake --build build --parallel
```

The `g++` package supplies the default `c++` driver used by the shortened
reviewer build, which is also supported verbatim:

```bash
cmake -B build
cmake --build build
```

The project also enables `-Wall -Wextra -Wpedantic -Wconversion
-Wsign-conversion -Wshadow` on project-owned GCC/Clang targets. To turn every
project warning into an error:

```bash
cmake -B build-strict -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=g++-12 \
  -DHFT_WARNINGS_AS_ERRORS=ON
cmake --build build-strict --parallel
```

## Live capture

An output directory must either not exist or be empty. Files are created
exclusively; an existing output file is never overwritten.

Single-symbol Spot capture for two minutes:

```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT \
  --duration 120 \
  --output-dir ./output
```

Multiple symbols on the one baseline connection (`shard_id=0`):

```bash
./build/binance_capture \
  --venue spot \
  --symbols BTCUSDT,ETHUSDT \
  --duration 120 \
  --output-dir ./output
```

USD-M capture:

```bash
./build/binance_capture \
  --venue usdm \
  --symbols BTCUSDT \
  --duration 120 \
  --output-dir ./output
```

`--venue` and `--symbols` are required. Symbols are a comma-separated list of
1 to 32 ASCII alphanumeric names, each 1 to 32 bytes. Input is normalized to
uppercase; empty elements and case-normalized duplicates are rejected. Each
symbol subscribes to:

- `<symbol>@depth@100ms`
- `<symbol>@depth5@100ms`
- `<symbol>@trade`

Omit `--duration` to run until `SIGINT`, `SIGTERM`, or a fatal error. Duration
is measured with `std::chrono::steady_clock` from run-loop start. A signal or
duration expiry enters the same idempotent asynchronous stop path; repeated
signals request the same orderly stop rather than aborting the process. A
duration-bounded run that reaches its deadline without ever opening a
WebSocket session drains and closes its output files but returns processing
exit code 5 with `failure.control=no_successful_connection`; it is not
reported as a successful empty capture. An operator-requested signal before
the first open session remains an orderly cancellation.

The fixed production combined-stream endpoints are:

| Venue | Endpoint |
|---|---|
| Spot | `wss://stream.binance.com:9443/stream?streams=` |
| USD-M | `wss://fstream.binance.com:443/public/stream?streams=` |

There is intentionally no production endpoint, CA-file, or insecure-TLS CLI
override. Endpoint and trust-root injection exists only in loopback tests.

### Output files

Live mode creates one audit/book pair per symbol using the current UTC date:

```text
output/
  market_data_spot_BTCUSDT_2026-08-03.csv
  market_data_spot_BTCUSDT_2026-08-03_orderbook.csv
```

With several symbols, each file contains that symbol's subsequence of the
single connection's processing order. `conn_seq` can therefore have gaps in
an individual file when intervening messages were routed elsewhere.

## CSV contracts

The exact market-data header is:

```text
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

There is one row for every recognized, syntactically valid event processed
from the socket, including schema-invalid events and trades. `payload_json`
is the minified inner Binance `data` object. Minification is lexical: key
order, duplicate keys, number spelling, and string escape spelling are
preserved. The field is RFC 4180 escaped; rows are never silently truncated.

The exact order-book header is:

```text
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

Every data row has exactly 26 fields. Prices and quantities use a fixed
`10^8` scale and are converted directly from decimal strings with checked
integer arithmetic; binary floating point is not used. Missing levels are
written as price `0`, size `0`. Bids are best/highest first and asks are
best/lowest first.

Accepted decimals are unsigned plain notation with an integer component and
an optional fractional component. Up to eight fractional digits are scaled;
additional fractional digits are accepted only when every discarded digit is
zero. Signs, exponent notation, whitespace, empty components, and checked
`int64_t` overflow are rejected. Prices must be positive; partial-refresh
quantities must be positive; differential quantity zero means delete.
Files use UTF-8/ASCII bytes, LF record endings, and no BOM.

Verify generated output:

```bash
head -2 ./output/market_data_spot_BTCUSDT_*.csv
head -2 ./output/market_data_spot_BTCUSDT_*_orderbook.csv
awk -F',' 'NR==2 { print NF }' ./output/*_orderbook.csv
# 26
```

### Timestamp, identifiers, and row codes

- Live `recv_tsec`/`recv_tnsec` are sampled once from `system_clock` after the
  complete logical WebSocket message has been read.
- `recv_tsec`/`tsec` are signed 64-bit epoch seconds and
  `recv_tnsec`/`tnsec` are signed 32-bit fields normalized to
  `[0, 1000000000)`, matching the CSV contract.
- An order-book row copies the corresponding audit row timestamp.
- Replay copies the persisted receive timestamp unchanged; fixed input bytes
  therefore produce byte-identical order-book output.
- `seqNo` starts at 1 independently in each symbol's order-book file and does
  not reset after reconnect.
- `id` is 32-bit FNV-1a of the normalized uppercase symbol, masked with
  `0x7fffffff`; zero maps to 1. Collisions within the configured set are
  rejected.
- `type=P` means a `depth5` partial refresh; `type=D` means an applied
  differential-depth update.
- `side=B` means only bids changed, `side=S` means only asks changed, and
  `side=N` means both/neither changed or the row is a symmetric refresh.
- Trades are audit-only. They do not mutate a differential book and do not
  emit order-book rows.

### Book and sequence policy

`depth5` replaces both visible sides atomically. A crossed refresh is a
schema rejection. Differential updates mutate the retained five levels;
quantity zero removes a price. The implementation uses fixed arrays, so the
work is `O(number_of_updates * 5)` and does not allocate while applying a
book event. A candidate crossed state is rejected and invalidates the book.

For Spot, after refresh update ID `L`:

1. A diff with `u <= L` is stale and ignored.
2. Otherwise it must satisfy `U <= L + 1 <= u`.
3. A gap invalidates the book; later diffs are ignored until another
   `depth5` refresh.

For USD-M, the first post-refresh diff uses the same bridge rule. After that
bridge, `pu` must equal the previously applied diff's `u`. The `pu` chain is
checked before stale classification: a redelivered post-bridge diff with
`u <= L` and mismatched `pu` invalidates the book. This deliberately favors
strict predecessor-chain integrity and cheap refresh-based reseeding.

The first successful connection uses `conn_epoch=0` and `conn_seq=1`.
Reconnect invalidates every routed symbol, increments `conn_epoch`, and
restarts `conn_seq` at 1. Recoverable transport/remote-close failures use
equal-jitter exponential backoff from 250 ms to 30 s. TLS trust, certificate,
hostname, SNI, and local configuration failures are fatal; there is no
insecure fallback. Binance ping frames are handled inside Beast's read path,
and automatic pong replies carry the received payload.

## Deterministic offline replay

Replay reads this program's exact audit schema and creates only regenerated
`*_orderbook.csv` files. It constructs no resolver, socket, TLS context,
WebSocket session, or reconnect timer.

```bash
rm -rf ./replay_output
./build/binance_capture \
  --replay ./testdata/replay/market_data_spot_BTCUSDT_fixture.csv \
  --output-dir ./replay_output

cmp \
  ./testdata/replay/expected/market_data_spot_BTCUSDT_fixture_orderbook.csv \
  ./replay_output/market_data_spot_BTCUSDT_fixture_orderbook.csv
```

Both commands return zero. Multiple independent symbol files can be replayed
in one process by repeating `--replay`:

```bash
rm -rf ./replay_output
./build/binance_capture \
  --replay ./testdata/replay/market_data_spot_BTCUSDT_fixture.csv \
  --replay ./testdata/replay/market_data_usdm_ETHUSDT_fixture.csv \
  --output-dir ./replay_output
```

Inputs must have distinct paths, symbols, and file stems. The output directory
must be distinct from every input directory and empty/nonexistent. Replay
validates RFC 4180 framing, the exact header and column count, numeric ranges,
venue/symbol stability, non-decreasing epochs, and strictly increasing
`conn_seq` within an epoch. Errors report the input path, logical record,
column where identifiable, and a stable category.

Committed fixture hashes are checked with:

```bash
(cd testdata/replay && sha256sum --check SHA256SUMS)
```

### Small matching sample

The committed Spot fixture is a compact audit sample. Its first event is:

```csv
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
1700000000,100,spot,depth5,0,0,1,BTCUSDT,"{""lastUpdateId"":100,""bids"":[[""100"",""1""]],""asks"":[[""101"",""1""]]}"
```

The corresponding first regenerated order-book row is:

```csv
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
1700000000,100,1,1747767916,P,N,10000000000,0,0,0,0,100000000,0,0,0,0,10100000000,0,0,0,0,100000000,0,0,0,0
```

### Two-minute live sample

On 2026-08-05, the public Spot command above was run for 120 seconds with
`BTCUSDT`. It completed one verified connection and processed 6,052 messages:
724 `depth_diff`, 725 `depth5`, and 4,603 `trade` events. It wrote all 6,052
audit rows and all 725 emitted book rows, with zero pre-audit or schema
rejections, sequence gaps, crossed books, unwritten rows, reconnects, or
backpressure pauses. The explicit-stop close deadline remained diagnostic
only: `connections.last_session_result=timeout`,
`connections.recoverable_failures=0`, and the run completed successfully.

Offline replay then read and processed all 6,052 audit rows and regenerated
all 725 book rows without network access. The live and replayed order-book
files were byte-identical, both with SHA-256
`2f59696f605d531e1ea78520ad155b424f453fcef2618e75f7e6b7f627c47e41`.
To keep the repository small, the matching live files are represented by this
one-event truncation as permitted by the assignment.

Audit event:

```csv
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
1785947196,375097924,spot,depth5,0,0,1,BTCUSDT,"{""lastUpdateId"":98268827980,""bids"":[[""64695.99000000"",""5.41310000""],[""64695.98000000"",""0.00025000""],[""64695.97000000"",""0.00016000""],[""64695.68000000"",""0.00018000""],[""64695.67000000"",""0.46474000""]],""asks"":[[""64696.00000000"",""0.62683000""],[""64696.01000000"",""0.00097000""],[""64696.02000000"",""0.00016000""],[""64696.60000000"",""0.00466000""],[""64697.14000000"",""0.00008000""]]}"
```

Matching order-book event:

```csv
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
1785947196,375097924,1,1747767916,P,N,6469599000000,6469598000000,6469597000000,6469568000000,6469567000000,541310000,25000,16000,18000,46474000,6469600000000,6469601000000,6469602000000,6469660000000,6469714000000,62683000,97000,16000,466000,8000
```

### Ubuntu 22.04 target validation

On 2026-08-06 UTC, commit `5f2038cb6eed57c892f52cc3268ad7fe569b35b2`
was cloned into a dedicated Oracle VirtualBox guest running Ubuntu Server
22.04.5 LTS with the Ubuntu 5.15.0-187-generic x86-64 kernel. This is a full
Linux VM, not WSL or a container. The environment used default GCC 11.4.0,
GCC 12.3.0, CMake 3.22.1, Ninja 1.10.1, Boost 1.74, and OpenSSL 3.0.2.

From that clean checkout, the PDF GCC 12 Release commands completed without
warnings and all seven registered tests passed. Separate clean builds also
passed with the default GCC 11 reviewer command, GCC 12 strict warnings as
errors, and GCC 12 ASan/UBSan with leak detection. Fixture hashes passed, and
replay under `strace -f -e trace=network` made zero network system calls.

Three location-allowed live checks used the same native Ubuntu guest:

- A 120-second Spot `BTCUSDT` run processed and wrote 4,150 audit events:
  862 `depth5`, 860 differential-depth, and 2,428 trade messages. One
  recoverable transport failure caused a successful reconnect and exercised
  epochs 0 and 1. The run wrote 862 book rows with no pre-audit/schema
  rejection, sequence gap, crossed book, backpressure event, or unwritten
  row. Replay regenerated the book byte-for-byte with SHA-256
  `98341f14e6e5b80eb1ba88748592955af31c4f7f52e7f21d117b311d82ec0f6e`.
- A 30-second USD-M `BTCUSDT` run processed 1,017 events and wrote 541 book
  rows: 254 refreshes, 287 applied differentials, and 476 audited trades. It
  had no sequence gap, schema failure, crossed book, backpressure event, or
  unwritten row. Replay was byte-identical with SHA-256
  `927243dcbd935b59a9aa4397dcec775bfb7c246dc922744396212d5f4e010836`.
- A 30-second Spot `BTCUSDT,ETHUSDT` run processed 2,032 events on one
  connection (`shard_id=0`) and wrote independent per-symbol audit/book
  pairs. It applied 587 refreshes and 292 differentials with no sequence gap,
  schema failure, crossed book, backpressure event, or unwritten row. Replay
  regenerated both books byte-for-byte; the BTCUSDT and ETHUSDT SHA-256 values
  were `ce8825747d73025a1710d6fdba468463d683e67e2e96c4282a10dfd750e12a3b`
  and `2b58c57d843206308976a71dcab1051f809c027c4bc85442db8e3f993323be59`.

The required hosted Ubuntu 22.04 workflow independently passes the PDF build,
default reviewer build, strict-warning build, and sanitizer matrix. Its public
Binance live step remains manual because hosted-runner egress may be rejected
by Binance; live evidence above was collected from the location-allowed VM.

## Threading, I/O, and resource policy

The live path has two ownership domains:

- One Boost.Asio I/O thread owns DNS/TCP/TLS/WebSocket state, routing,
  parsing, per-symbol books, timers, and the sole queue producer.
- One writer thread is the sole queue consumer and owns every file handle and
  per-file aggregation buffer.

The handoff is a 4096-slot SPSC ring additionally bounded to 64 MiB of logical
row bytes. Producer and consumer sequence/byte counters are single-writer
atomics; no contended mutex is acquired per event. The writer aggregates up to
256 KiB per file and flushes on capacity, one-second age, and shutdown. Rows
larger than the aggregation buffer use a checked direct-write loop.

At 75% queue occupancy (records or bytes), the session stops issuing new
reads. It resumes below 50%; failure to recover within five seconds is fatal.
The already-started read is accounted for by queue headroom. Shutdown stops
the producer, drains and joins the writer, checks flush/close results, and
returns nonzero if a row cannot be written. Flush/close guarantees delivery
to the kernel page cache on clean process exit; it deliberately does not call
`fsync`/`fdatasync`, so power-loss durability is not claimed.

| Limit | Baseline value |
|---|---:|
| Symbols on the one connection | 32 |
| Derived streams | 96 |
| Combined WebSocket target | 8192 bytes |
| Envelope stream name | 128 bytes |
| Complete logical WebSocket message / decoded replay payload | 1 MiB |
| JSON container nesting | 64 |
| Combined differential updates | 16,384 |
| Formatted CSV record | 3 MiB |
| Writer queue | 4096 records and 64 MiB |

Oversized, binary, malformed, or schema-invalid input is counted and never
silently truncated. Binary and oversized complete-message terminations share
a three-consecutive-termination circuit breaker so a persistently wrong
server cannot cause an unbounded reconnect loop.

## Measured performance note

This is reviewer evidence, not a stretch runtime feature or a production
capacity claim. `hft_core_benchmark` is excluded from the default build. It
reuses one realistic five-bid/five-ask Spot `depth5` payload and measures the
shared payload parser, fixed-point conversion, book replacement, and both CSV
formatters. It excludes envelope extraction, socket/TLS, the SPSC handoff,
writer-thread scheduling, and disk I/O. Each event is timed with
`steady_clock`; the latency vector and sorting occur in the benchmark harness.

Reproduce it with:

```bash
cmake --build build-strict --target hft_core_benchmark --parallel
/usr/bin/time -f 'max_rss_kib=%M' \
  ./build-strict/hft_core_benchmark 1000000
sha256sum benchmarks/core_benchmark.cpp
```

Five one-million-event runs were made on 2026-08-03. The table reports the
median of the five reported results; peak RSS is from a representative run.

| Item | Value |
|---|---:|
| Host | 12th Gen Intel Core i7-1255U, 6 cores / 12 logical CPUs, 7.6 GiB WSL allocation |
| OS/kernel | Ubuntu 24.04.2 WSL2, Linux 6.18.33.2 |
| Compiler/build | GCC 12.4.0, C++17, Release, project strict warnings with `-Werror` |
| Workload | 1,000,000 measured events after 10,000 warm-up events; 218-byte payload |
| Core throughput | 622,779 events/s |
| Per-event p50 | 1,556 ns |
| Per-event p95 | 1,894 ns |
| Per-event p99 | 2,903 ns |
| Representative peak RSS | 12,520 KiB (includes the 1,000,000-entry latency vector) |
| Benchmark-source SHA-256 | `1e32c99cf16c4eec6c9a93f5e4ca4cce1ae43185edb48b31f5ecd0156bcf5f2c` |

Run-to-run throughput ranged from 576,023 to 675,678 events/s. No CPU pinning,
frequency control, `-march=native`, allocator interposition, or general-purpose
allocation-count claim was used. These numbers establish reproducible core
headroom and make the measurement boundary explicit; they do not represent
end-to-end live latency or durable-write latency.

## Metrics

After initialized live or replay runs, the process prints exactly one stable
`METRICS_BEGIN version=1` / `METRICS_END` block to `stderr`. Values are
decimal integers or stable enum strings. Important groups are:

| Prefix | Meaning |
|---|---|
| `run.*` | mode, final status/exit code, stop source, and signal counts |
| `source.*` | complete live messages or replay rows read |
| `connections.*` | attempts, successful sessions, reconnects, recoverable failures, last observed session result |
| `events.*` | pre-audit reasons, accepted/schema events, book outcomes, trades |
| `writer.*` | rows enqueued, written, and provably left unwritten |
| `policy.*` | binary/oversized messages and breaker trips |
| `backpressure.*` | producer pause/resume transitions |
| `failure.*` | stable capture, control, session, writer, or replay category |

For a successful live run:

```text
source.complete_messages =
    writer.audit_rows_enqueued + events.pre_audit_rejections
writer.audit_rows_enqueued = writer.audit_rows_written
writer.book_rows_enqueued = writer.book_rows_written
writer.audit_rows_unwritten = writer.book_rows_unwritten = 0
```

A successful deterministic fixture replay emits, in part:

```text
METRICS_BEGIN version=1
run.mode=replay
run.status=success
run.exit_code=0
source.complete_messages=0
source.replay_files=1
source.replay_rows_read=6
connections.attempts=0
events.processed=6
writer.audit_rows_written=0
writer.book_rows_enqueued=3
writer.book_rows_written=3
writer.book_rows_unwritten=0
failure.replay=none
METRICS_END
```

`policy.binary_messages` is a diagnostic subcounter already included once in
`events.pre_audit_rejections`. `connections.last_session_result` preserves the
last observed session outcome, including a recovered error or orderly
cancellation. `failure.session` is non-`none` only when the terminal capture
failure is attributed to the session or the message-policy breaker; therefore
every successful duration, signal, or reconnect-backoff stop reports
`failure.session=none`. If the peer rejects or does not complete its close
handshake after an explicit stop, `connections.last_session_result` records
`close_failure` or `timeout` while checked writer drain still determines
capture success. These close-stage lifecycle outcomes do not increment
`connections.recoverable_failures`; no reconnect is attempted after stopping.
Fatal writer runs retain enqueued/written/unwritten counts instead of claiming
the success equalities. A duration expiry with
`connections.successful=0` reports `run.status=fatal`, `run.exit_code=5`, and
`failure.control=no_successful_connection` after checked writer drain.

## Tests and review evidence

Run the assignment's public test interface:

```bash
cmake --build build --target tests
./build/tests/unit_tests
```

Or run every registered process/integration test directly:

```bash
ctest --test-dir build --output-on-failure
```

The suite covers fixed-point boundaries, exact headers/RFC escaping, Spot and
USD-M payload validation, depth replacement/diff deletion, venue-specific
sequence gaps and recovery, reconnect epochs, queue limits/backpressure,
partial and oversized WebSocket reads, ping/pong, binary-message breaker,
TLS trust and hostname failures, signal/duration shutdown, public CLI live
capture, replay failures, byte-identical replay outputs, and fixture hashes.
The pinned-parser contract test compiles and executes simdjson 3.6.4
`ondemand::value::raw_json()` and the four-argument output-buffer `minify`
over byte-faithfulness fixtures; dependency upgrades must preserve that path.

Warning-clean sanitizer build:

```bash
cmake -B build-sanitize -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_COMPILER=g++-12 \
  -DHFT_WARNINGS_AS_ERRORS=ON \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
cmake --build build-sanitize --target tests --parallel
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1
export UBSAN_OPTIONS=halt_on_error=1
./build-sanitize/tests/unit_tests
ctest --test-dir build-sanitize --output-on-failure
```

The required Ubuntu 22.04 CI performs the PDF GCC 12/Ninja build, the clean
default GCC 11 build, a strict GCC 12 `-Werror` build, and GCC 12
ASan/UBSan/leak-detection tests. No Clang or ThreadSanitizer result is claimed.

Check tracked files for forbidden credential paths, private-key material, and
credential-looking assignments in source/build/test configuration:

```bash
./scripts/check_no_secrets.sh
# secret-scan: clean
```

The assignment's broad `git grep -i "api_key\|secret\|password"` also matches
security prose and the deliberate ignore rule `config/secrets.json`; those
text matches are not credentials. The committed scanner checks credential
assignments in executable/configuration material and independently checks all
tracked files for private-key headers and forbidden filenames. Public Binance
market data requires no credentials or `.env` file.

The detailed ownership, parser-lifetime, sequencing, reconnect, and failure
contracts are in [DESIGN.md](DESIGN.md).

## How to Submit on GitHub

From the project root, verify the intended repository and clean state:

```bash
git remote -v
git status --short
```

Push the reviewed main branch and create the requested annotated submission
tag only when the submission is final:

```bash
git push origin main
git tag -a v1.0.0 -m "Submission v1.0.0"
git push origin v1.0.0
```

Never commit API keys, credentials, private keys, `.env` files, generated
build directories, or runtime output directories.
