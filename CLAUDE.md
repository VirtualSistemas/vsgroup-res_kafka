# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is this

`res_kafka` is an Asterisk resource module that wraps librdkafka, providing Kafka producer/consumer APIs to other Asterisk modules. It is a shared object (`.so`) loaded by Asterisk at runtime.

## Build & Install

This module **must be compiled on a Linux machine** with Asterisk headers and librdkafka-dev installed. It cannot be built on macOS.

```bash
make                  # Build res_kafka.so
make test             # Build test_res_kafka.so (requires TEST_FRAMEWORK)
make install          # Install module + XML docs to Asterisk module dir
make samples          # Install kafka.conf.sample to /etc/asterisk/
make clean            # Remove .o and .so files
```

## Architecture

Three source files compose the module:

- **`res_kafka.c`** — Main module: producer/consumer lifecycle, AO2 hash containers for caching instances, background poll thread (100ms cycle), librdkafka callbacks (log, error, delivery report, rebalance), rate-limiting for log flooding, and the public API (`ast_kafka_produce`, `ast_kafka_get_producer`, `ast_kafka_get_consumer`, `ast_kafka_consumer_subscribe`, `ast_kafka_ensure_topic`).
- **`kafka/config.c`** — ACO-based configuration parser for `kafka.conf`. Handles `[general]` section and multiple `[connection]` sections with sorcery-style `type = connection` matching.
- **`kafka/cli.c`** — CLI commands (`kafka show status`, `kafka show connection`, `kafka show consumers`, `kafka test send`) with tab-completion.

Key headers:
- **`asterisk/kafka.h`** — Public API header that dependent modules include.
- **`kafka/internal.h`** — Internal structs (`kafka_conf_connection`, `kafka_conf_general`) and function declarations shared between the three source files.

## Key patterns

- Producers and consumers are **lazily created** on first call to `ast_kafka_get_producer()`/`ast_kafka_get_consumer()` and cached in AO2 hash containers.
- A single background `poll_thread` iterates all active producers (`rd_kafka_poll`) and consumers (`rd_kafka_consumer_poll`) every 100ms.
- The error/log callbacks use a per-client **rate-limiting mechanism** (`kafka_ratelimit_allow`) to suppress repeated messages during broker outages (30s window).
- Configuration uses Asterisk's **ACO framework** (aco_info, aco_type, aco_option). Config object is `kafka_conf` with nested `kafka_conf_general` and `ao2_container` of `kafka_conf_connection`.
- All librdkafka config options are mapped through helper macros `KAFKA_CONF_SET` / `KAFKA_CONF_SET_INT` in `res_kafka.c`.

## Testing

`test_res_kafka.c` contains 14 `AST_TEST_DEFINE` tests. These run inside a live Asterisk instance with `TEST_FRAMEWORK` enabled and require a real Kafka broker configured as connection `test-kafka` in `kafka.conf`. Tests are loaded via `module load test_res_kafka.so` and executed with `test execute category /res/kafka/`.

## Configuration

Config file: `/etc/asterisk/kafka.conf` (sample: `kafka.conf.sample`). XML documentation lives in `documentation/res_kafka_config-en_US.xml` — this file **must stay in sync** with the `DOCUMENTATION` block at the top of `res_kafka.c`.
