# res_kafka

Asterisk resource module that provides Kafka producer and consumer APIs built on [librdkafka](https://github.com/confluentinc/librdkafka).

Other Asterisk modules (CDR backends, event publishers, event subscribers, etc.) use `res_kafka` to produce and consume messages from Kafka topics without dealing with librdkafka directly.

## Features

- Named connections configured in `kafka.conf` — multiple brokers/clusters supported
- **Producer**: thread-safe, non-blocking, with automatic QUEUE_FULL retry and flush
- **Consumer**: callback-based model — subscribe to topics and receive messages via callback from the internal poll thread
- **Admin**: topic creation via librdkafka Admin API
- Lazy initialization — producers and consumers are created on first use
- Background poll thread (100 ms) for delivery reports, message consumption, and librdkafka housekeeping
- Full security support: SASL (PLAIN, SCRAM-SHA-256/512) and SSL/mTLS
- Compression: gzip, snappy, lz4, zstd
- Reliability: idempotent producer, configurable acks, automatic reconnection
- CLI commands for status inspection, consumer monitoring, and test message sending

## Prerequisites

- Asterisk 18+
- librdkafka development headers

### Debian/Ubuntu

```bash
apt-get install librdkafka-dev
```

### RHEL/CentOS/Rocky

```bash
yum install librdkafka-devel
```

## Building

```bash
make
```

## Installation

```bash
make install
make samples
```

Restart Asterisk if you want the built-in XML documentation to be available via `core show help`.

## Configuration

Edit `/etc/asterisk/kafka.conf`:

```ini
[general]
enabled = yes

[my-kafka]
type = connection
brokers = kafka1:9092,kafka2:9092   ; Comma-separated broker list
                                     ; Default: localhost:9092
client_id = asterisk                 ; Client identifier sent to the broker
                                     ; Default: asterisk

;; --- Network ---
request_timeout_ms = 5000            ; Network request timeout
                                     ; Default: 5000
message_timeout_ms = 300000          ; How long messages are buffered before
                                     ; delivery failure (broker outage tolerance)
                                     ; Default: 300000 (5 minutes)
reconnect_backoff_ms = 100           ; Initial reconnect backoff
                                     ; Default: 100
reconnect_backoff_max_ms = 10000     ; Maximum reconnect backoff
                                     ; Default: 10000

;; --- Security (optional) ---
;security_protocol = plaintext       ; plaintext | ssl | sasl_plaintext | sasl_ssl
;sasl_mechanisms =                   ; PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
;sasl_username =
;sasl_password =
;ssl_ca_location =                   ; Path to CA certificate
;ssl_certificate_location =          ; Path to client certificate (mTLS)
;ssl_key_location =                  ; Path to client private key (mTLS)

;; --- Producer options ---
message_max_bytes = 1000000          ; Maximum message size in bytes
                                     ; Default: 1000000
compression_codec = none             ; none | gzip | snappy | lz4 | zstd
                                     ; Default: none
linger_ms = 5                        ; Batching delay in ms
                                     ; Default: 5
batch_num_messages = 10000           ; Max messages per batch
                                     ; Default: 10000
batch_size = 1000000                 ; Max batch size in bytes
                                     ; Default: 1000000
acks = -1                            ; Required broker acks: -1=all, 0=none, 1=leader
                                     ; Default: -1
enable_idempotence = no              ; Exactly-once semantics
                                     ; Default: no

;; --- Consumer options (required for consumer mode) ---
;group_id = asterisk-group           ; Consumer group ID (required)
;auto_offset_reset = latest          ; earliest | latest | none
                                     ; Default: latest
;enable_auto_commit = yes            ; Auto-commit offsets
                                     ; Default: yes
;auto_commit_interval_ms = 5000      ; Auto-commit interval in ms
                                     ; Default: 5000
;session_timeout_ms = 45000          ; Consumer session timeout
                                     ; Default: 45000
;max_poll_interval_ms = 300000       ; Max time between poll calls
                                     ; Default: 300000

;; --- Debug ---
;debug = broker,topic,msg            ; librdkafka debug contexts
```

You can define multiple `[connection]` sections with different names to connect to different Kafka clusters. The same connection can be used for both producing and consuming — consumer mode requires `group_id` to be set.

## Loading

```
asterisk -rx "module load res_kafka.so"
```

## CLI Commands

```
kafka show status                          Show all connections and brokers
kafka show connection <name>               Show details of a specific connection
kafka show consumers                       Show all active consumers
kafka show consumer <name>                 Show details of a specific consumer
kafka test send connection <name> topic <topic> message <msg> [key <key>]
                                           Send a test message to a topic
```

### Examples

```
CLI> kafka show status
Connections:
Name            Brokers
my-kafka        kafka1:9092,kafka2:9092

CLI> kafka show connection my-kafka
Name:              my-kafka
Brokers:           kafka1:9092,kafka2:9092
Client ID:         asterisk
Message max bytes: 1000000
Request timeout:   5000 ms
Message timeout:   300000 ms

CLI> kafka test send connection my-kafka topic test_topic message "hello world"
Message sent successfully

CLI> kafka show consumers
Name            Group ID                  Subscribed
my-kafka        asterisk-group            Yes

CLI> kafka show consumer my-kafka
Name:              my-kafka
Group ID:          asterisk-group
Subscribed:        Yes
```

## API for Other Modules

Modules that depend on `res_kafka` should copy `asterisk/kafka.h` into their project and add `<depend>res_kafka</depend>` to their `MODULEINFO`.

### Producer

```c
/* Get (or lazily create) a producer by connection name. AO2-managed. */
struct ast_kafka_producer *ast_kafka_get_producer(const char *name);

/* Produce a message. Copies payload internally (non-blocking). */
int ast_kafka_produce(struct ast_kafka_producer *producer,
    const char *topic, const char *key,
    const void *payload, size_t len);
```

### Consumer

```c
/* Callback invoked from the poll thread for each received message. */
typedef void (*ast_kafka_message_cb)(
    const char *topic, int32_t partition, int64_t offset,
    const void *payload, size_t len,
    const void *key, size_t key_len,
    void *userdata);

/* Get (or lazily create) a consumer by connection name. AO2-managed.
 * The connection must have group_id configured. */
struct ast_kafka_consumer *ast_kafka_get_consumer(const char *name);

/* Subscribe to topics (comma-separated). Messages arrive via callback. */
int ast_kafka_consumer_subscribe(struct ast_kafka_consumer *consumer,
    const char *topics,
    ast_kafka_message_cb callback,
    void *userdata);

/* Unsubscribe from all topics. */
int ast_kafka_consumer_unsubscribe(struct ast_kafka_consumer *consumer);
```

### Admin

```c
/* Ensure a topic exists, creating it if necessary. */
int ast_kafka_ensure_topic(struct ast_kafka_producer *producer,
    const char *topic, int num_partitions, int replication_factor);
```

## Architecture

```
kafka.conf
    |
    v
kafka/config.c       ACO-based configuration (general + N connections)
    |
    v
res_kafka.c          Producer + Consumer lifecycle, poll thread, public APIs
    |                 Exports: producer, consumer, and admin functions
    v
kafka/cli.c          CLI commands (show status/connection/consumers, test send)
```

| Component | Responsibility |
|-----------|---------------|
| `kafka/config.c` | Parses `kafka.conf` using Asterisk Config Options (ACO). Supports `[general]` + multiple `[connection]` sections with sorcery-style `type = connection` matching. |
| `res_kafka.c` | Creates librdkafka producers and consumers on demand, caches them in AO2 hash containers. Runs a background thread calling `rd_kafka_poll()` / `rd_kafka_consumer_poll()` every 100 ms for delivery callbacks and message consumption. |
| `kafka/cli.c` | Registers CLI commands with tab-completion for connection names. |

## Project Structure

```
vsgroup-res_kafka/
├── res_kafka.c                   Main module (producer + consumer + admin APIs)
├── kafka/
│   ├── internal.h                Internal structs and function declarations
│   ├── config.c                  Configuration parsing (ACO)
│   └── cli.c                     CLI commands
├── asterisk/
│   └── kafka.h                   Public API header (for dependent modules)
├── documentation/
│   └── res_kafka_config-en_US.xml
├── kafka.conf.sample             Sample configuration
├── Makefile
├── LICENSE                       GPLv2
└── AUTHORS
```

## License

GNU General Public License Version 2. See [LICENSE](LICENSE).
