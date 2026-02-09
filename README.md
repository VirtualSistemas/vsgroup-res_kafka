# res_kafka

Asterisk resource module that provides a Kafka producer API built on [librdkafka](https://github.com/confluentinc/librdkafka).

Other Asterisk modules (CDR backends, event publishers, etc.) use `res_kafka` to produce messages to Kafka topics without dealing with librdkafka directly.

## Features

- Named connections configured in `kafka.conf` — multiple brokers/clusters supported
- Thread-safe producer instances managed via AO2 (Asterisk Object Model)
- Lazy producer creation — connections are established on first use
- Background poll thread for delivery reports and internal librdkafka housekeeping
- Automatic QUEUE_FULL retry with flush before failing
- Automatic reconnection handled by librdkafka (buffered for up to `message_timeout_ms`)
- CLI commands for status inspection and test message sending

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
message_max_bytes = 1000000          ; Maximum message size in bytes
                                     ; Default: 1000000
request_timeout_ms = 5000            ; Network request timeout
                                     ; Default: 5000
message_timeout_ms = 300000          ; How long messages are buffered before
                                     ; delivery failure (broker outage tolerance)
                                     ; Default: 300000 (5 minutes)
```

You can define multiple `[connection]` sections with different names to connect to different Kafka clusters.

## Loading

```
asterisk -rx "module load res_kafka.so"
```

## CLI Commands

```
kafka show status                          Show all connections and brokers
kafka show connection <name>               Show details of a specific connection
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

CLI> kafka test send connection my-kafka topic test_topic message "hello" key my-key
Message sent successfully
```

## API for Other Modules

Modules that depend on `res_kafka` use two functions declared in `asterisk/kafka.h`:

```c
/* Get (or lazily create) a producer by connection name. AO2-managed. */
struct ast_kafka_producer *ast_kafka_get_producer(const char *name);

/* Produce a message. Copies payload internally (non-blocking). */
int ast_kafka_produce(struct ast_kafka_producer *producer,
    const char *topic, const char *key,
    const void *payload, size_t len);
```

Modules should copy `asterisk/kafka.h` into their project and add `<depend>res_kafka</depend>` to their `MODULEINFO`.

## Architecture

```
kafka.conf
    |
    v
kafka/config.c       ACO-based configuration (general + N connections)
    |
    v
res_kafka.c          Producer lifecycle, poll thread, produce API
    |                 Exports: ast_kafka_get_producer(), ast_kafka_produce()
    v
kafka/cli.c          CLI commands (show status, show connection, test send)
```

| Component | Responsibility |
|-----------|---------------|
| `kafka/config.c` | Parses `kafka.conf` using Asterisk Config Options (ACO). Supports `[general]` + multiple `[connection]` sections with sorcery-style `type = connection` matching. |
| `res_kafka.c` | Creates librdkafka producers on demand, caches them in an AO2 hash container. Runs a background thread calling `rd_kafka_poll()` every 100ms for delivery callbacks. |
| `kafka/cli.c` | Registers CLI commands with tab-completion for connection names. |

## Project Structure

```
vsgroup-res_kafka/
├── res_kafka.c                   Main module (producer API + poll thread)
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
