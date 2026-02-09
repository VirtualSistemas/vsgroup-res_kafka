/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright 2026 VSGroup (Virtual Sistemas e Tecnologia Ltda)  (see the AUTHORS file)
 *
 * See http://www.asterisk.org for more information about
 * the Asterisk project. Please do not directly contact
 * any of the maintainers of this project for assistance;
 * the project provides a web site, mailing lists and IRC
 * channels for your use.
 *
 * This program is free software, distributed under the terms of
 * the GNU General Public License Version 2. See the LICENSE file
 * at the top of the source tree.
 */

#ifndef _ASTERISK_KAFKA_INTERNAL_H_
#define _ASTERISK_KAFKA_INTERNAL_H_

#include "asterisk/stringfields.h"

/*! \file
 *
 * \brief Internal API's for res_kafka.
 */

/*! @{ */

/*!
 * \brief Register the kafka commands
 *
 * \return 0 on success.
 * \return -1 on failure.
 */
int kafka_cli_register(void);

/*!
 * \brief Unregister the kafka commands
 *
 * \return 0 on success.
 * \return -1 on failure.
 */
int kafka_cli_unregister(void);

/*! @} */

/*! @{ */

struct kafka_conf_general;

/*! \brief Kafka configuration structure */
struct kafka_conf {
	/*! The general section configuration options */
	struct kafka_conf_general *general;
	/*! Configured connections */
	struct ao2_container *connections;
};

/*! \brief General configuration options for Kafka */
struct kafka_conf_general {
	/*! Enabled by default, disabled if false. */
	int enabled;
};

/*! \brief Kafka per-connection configuration */
struct kafka_conf_connection {
	AST_DECLARE_STRING_FIELDS(
	/*! The name of the connection */
	AST_STRING_FIELD(name);
	/*! The broker list (e.g. "localhost:9092") */
	AST_STRING_FIELD(brokers);
	/*! The client ID */
	AST_STRING_FIELD(client_id);
	/*! Security protocol: plaintext, ssl, sasl_plaintext, sasl_ssl */
	AST_STRING_FIELD(security_protocol);
	/*! SASL mechanism (e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) */
	AST_STRING_FIELD(sasl_mechanisms);
	/*! SASL username */
	AST_STRING_FIELD(sasl_username);
	/*! SASL password */
	AST_STRING_FIELD(sasl_password);
	/*! Path to CA certificate file for SSL */
	AST_STRING_FIELD(ssl_ca_location);
	/*! Path to client certificate for SSL */
	AST_STRING_FIELD(ssl_certificate_location);
	/*! Path to client private key for SSL */
	AST_STRING_FIELD(ssl_key_location);
	/*! Compression codec: none, gzip, snappy, lz4, zstd */
	AST_STRING_FIELD(compression_codec);
	/*! librdkafka debug contexts (comma-separated) */
	AST_STRING_FIELD(debug);
	/*! Consumer group ID (required for consumer) */
	AST_STRING_FIELD(group_id);
	/*! Consumer auto offset reset: earliest, latest, none */
	AST_STRING_FIELD(auto_offset_reset);
	);

	/*! Maximum message size in bytes */
	int message_max_bytes;
	/*! Request timeout in milliseconds */
	int request_timeout_ms;
	/*! Message timeout in milliseconds (how long to buffer before giving up) */
	int message_timeout_ms;
	/*! Compression level (-1 = codec default) */
	int compression_level;
	/*! Delay in ms to wait for messages to batch (queue.buffering.max.ms) */
	int linger_ms;
	/*! Maximum number of messages batched in one MessageSet */
	int batch_num_messages;
	/*! Maximum size of a batch in bytes */
	int batch_size;
	/*! Maximum number of messages in the producer queue */
	int queue_buffering_max_messages;
	/*! Maximum total size of messages in the producer queue (kbytes) */
	int queue_buffering_max_kbytes;
	/*! Required broker acks: -1=all, 0=none, 1=leader */
	int acks;
	/*! Maximum number of send retries */
	int retries;
	/*! Enable idempotent producer (exactly-once semantics) */
	int enable_idempotence;
	/*! Initial reconnect backoff in milliseconds */
	int reconnect_backoff_ms;
	/*! Maximum reconnect backoff in milliseconds */
	int reconnect_backoff_max_ms;
	/*! Enable automatic offset commit (consumer) */
	int enable_auto_commit;
	/*! Auto commit interval in milliseconds (consumer) */
	int auto_commit_interval_ms;
	/*! Session timeout in milliseconds (consumer) */
	int session_timeout_ms;
	/*! Maximum poll interval in milliseconds (consumer) */
	int max_poll_interval_ms;
};

/*!
 * \brief Initialize Kafka configuration.
 *
 * \return 0 on success.
 * \return -1 on failure.
 */
int kafka_config_init(void);

/*!
 * \brief Reload Kafka configuration.
 *
 * \return 0 on success.
 * \return -1 on failure.
 */
int kafka_config_reload(void);

/*!
 * \brief Destroy Kafka configuration.
 */
void kafka_config_destroy(void);

/*!
 * \brief Get the Kafka configuration object.
 *
 * This object is AO2 managed, and should be freed with \ref ao2_cleanup().
 *
 * \return Kafka configuration.
 * \return \c NULL on error.
 */
struct kafka_conf *kafka_config_get(void);

/*!
 * \brief Get the Kafka configuration object for a connection.
 *
 * This object is AO2 managed, and should be freed with \ref ao2_cleanup().
 *
 * \return Kafka connection configuration.
 * \return \c NULL on error, or if connection is not configured.
 */
struct kafka_conf_connection *kafka_config_get_connection(
	const char *name);

/*! @} */

/*! @{ */

struct ast_kafka_consumer;

/*!
 * \brief Callback for iterating active consumers.
 *
 * \param consumer The consumer object.
 * \param arg User-supplied argument.
 * \return 0 to continue, non-zero to stop.
 */
typedef int (*kafka_consumer_foreach_cb)(struct ast_kafka_consumer *consumer, void *arg);

/*!
 * \brief Iterate all active consumers.
 *
 * \param cb Callback invoked for each consumer.
 * \param arg User-supplied argument passed to callback.
 */
void kafka_foreach_consumer(kafka_consumer_foreach_cb cb, void *arg);

/*!
 * \brief Get the name of a consumer.
 *
 * \param consumer The consumer object.
 * \return The consumer name string.
 */
const char *kafka_consumer_get_name(struct ast_kafka_consumer *consumer);

/*!
 * \brief Check if a consumer is subscribed.
 *
 * \param consumer The consumer object.
 * \return Non-zero if subscribed, 0 otherwise.
 */
int kafka_consumer_is_subscribed(struct ast_kafka_consumer *consumer);

/*! @} */

#endif /* _ASTERISK_KAFKA_INTERNAL_H_ */
