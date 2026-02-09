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

/*! \file
 *
 * \brief Kafka producer and consumer APIs.
 *
 * This is a wrapper around <a href="https://github.com/confluentinc/librdkafka">librdkafka</a>,
 * with additional features for connection management using the Asterisk AO2 framework.
 */

/*** MODULEINFO
   <depend>rdkafka</depend>
   <support_level>core</support_level>
 ***/

/*** DOCUMENTATION
   <configInfo name="res_kafka" language="en_US">
       <synopsis>Kafka producer and consumer API</synopsis>
       <configFile name="kafka.conf">
           <configObject name="general">
               <synopsis>General configuration settings</synopsis>
               <configOption name="enabled">
                   <synopsis>Enable/disable the Kafka module</synopsis>
                   <description>
                       <para>This option enables or disables the Kafka module.</para>
                   </description>
               </configOption>
           </configObject>

           <configObject name="connection">
               <synopsis>Per-connection configuration settings</synopsis>
               <configOption name="type">
                   <synopsis>Define this configuration section as a connection.</synopsis>
                   <description>
                       <enumlist>
                           <enum name="connection"><para>Configure this section as a <replaceable>connection</replaceable></para></enum>
                       </enumlist>
                   </description>
               </configOption>
               <configOption name="brokers">
                   <synopsis>Broker list to connect to</synopsis>
                   <description>
                       <para>Comma-separated list of Kafka brokers in the form <literal>host[:port]</literal>. Defaults to <literal>localhost:9092</literal>.</para>
                   </description>
               </configOption>
               <configOption name="client_id">
                   <synopsis>Client identifier</synopsis>
                   <description>
                       <para>Client identifier string sent to the broker. Defaults to <literal>asterisk</literal>.</para>
                   </description>
               </configOption>
               <configOption name="message_max_bytes">
                   <synopsis>Maximum message size in bytes</synopsis>
                   <description>
                       <para>Maximum size for a message to be produced. Defaults to 1000000.</para>
                   </description>
               </configOption>
               <configOption name="request_timeout_ms">
                   <synopsis>Request timeout in milliseconds</synopsis>
                   <description>
                       <para>The timeout for network requests. Defaults to 5000.</para>
                   </description>
               </configOption>
               <configOption name="message_timeout_ms">
                   <synopsis>Message delivery timeout in milliseconds</synopsis>
                   <description>
                       <para>How long a produced message can be buffered before it is
                       considered a delivery failure. This controls how long messages
                       are retained during broker outages. Defaults to 300000 (5 minutes).</para>
                   </description>
               </configOption>
               <configOption name="security_protocol">
                   <synopsis>Protocol used to communicate with brokers</synopsis>
                   <description>
                       <para>Security protocol: <literal>plaintext</literal>, <literal>ssl</literal>,
                       <literal>sasl_plaintext</literal>, or <literal>sasl_ssl</literal>.
                       Defaults to <literal>plaintext</literal>.</para>
                   </description>
               </configOption>
               <configOption name="sasl_mechanisms">
                   <synopsis>SASL mechanism for authentication</synopsis>
                   <description>
                       <para>SASL mechanism to use (e.g. <literal>PLAIN</literal>,
                       <literal>SCRAM-SHA-256</literal>, <literal>SCRAM-SHA-512</literal>).
                       Empty by default (no SASL).</para>
                   </description>
               </configOption>
               <configOption name="sasl_username">
                   <synopsis>SASL username</synopsis>
                   <description>
                       <para>Username for SASL authentication. Empty by default.</para>
                   </description>
               </configOption>
               <configOption name="sasl_password">
                   <synopsis>SASL password</synopsis>
                   <description>
                       <para>Password for SASL authentication. Empty by default.</para>
                   </description>
               </configOption>
               <configOption name="ssl_ca_location">
                   <synopsis>Path to CA certificate file for SSL</synopsis>
                   <description>
                       <para>File path to the CA certificate(s) for verifying the broker certificate.
                       Empty by default.</para>
                   </description>
               </configOption>
               <configOption name="ssl_certificate_location">
                   <synopsis>Path to client certificate for mTLS</synopsis>
                   <description>
                       <para>File path to the client certificate for mutual TLS authentication.
                       Empty by default.</para>
                   </description>
               </configOption>
               <configOption name="ssl_key_location">
                   <synopsis>Path to client private key for mTLS</synopsis>
                   <description>
                       <para>File path to the client private key for mutual TLS authentication.
                       Empty by default.</para>
                   </description>
               </configOption>
               <configOption name="compression_codec">
                   <synopsis>Message compression codec</synopsis>
                   <description>
                       <para>Compression codec: <literal>none</literal>, <literal>gzip</literal>,
                       <literal>snappy</literal>, <literal>lz4</literal>, or <literal>zstd</literal>.
                       Defaults to <literal>none</literal>.</para>
                   </description>
               </configOption>
               <configOption name="compression_level">
                   <synopsis>Compression level</synopsis>
                   <description>
                       <para>Codec-dependent compression level. -1 uses the codec default.
                       Defaults to -1.</para>
                   </description>
               </configOption>
               <configOption name="linger_ms">
                   <synopsis>Producer batching delay in milliseconds</synopsis>
                   <description>
                       <para>How long to wait for additional messages before sending a batch
                       (librdkafka queue.buffering.max.ms). Defaults to 5.</para>
                   </description>
               </configOption>
               <configOption name="batch_num_messages">
                   <synopsis>Maximum number of messages per batch</synopsis>
                   <description>
                       <para>Maximum number of messages batched in one MessageSet.
                       Defaults to 10000.</para>
                   </description>
               </configOption>
               <configOption name="batch_size">
                   <synopsis>Maximum batch size in bytes</synopsis>
                   <description>
                       <para>Maximum total size of messages in a single batch.
                       Defaults to 1000000.</para>
                   </description>
               </configOption>
               <configOption name="queue_buffering_max_messages">
                   <synopsis>Maximum messages in producer queue</synopsis>
                   <description>
                       <para>Maximum number of messages allowed in the producer queue.
                       Defaults to 100000.</para>
                   </description>
               </configOption>
               <configOption name="queue_buffering_max_kbytes">
                   <synopsis>Maximum producer queue size in kilobytes</synopsis>
                   <description>
                       <para>Maximum total message size in the producer queue (in KB).
                       Defaults to 1048576.</para>
                   </description>
               </configOption>
               <configOption name="acks">
                   <synopsis>Required broker acknowledgements</synopsis>
                   <description>
                       <para>Number of acknowledgements the leader broker must receive:
                       <literal>-1</literal> (all in-sync replicas), <literal>0</literal> (none),
                       <literal>1</literal> (leader only). Defaults to -1.</para>
                   </description>
               </configOption>
               <configOption name="retries">
                   <synopsis>Maximum send retries</synopsis>
                   <description>
                       <para>How many times to retry sending a failing message.
                       Defaults to 2147483647 (effectively infinite).</para>
                   </description>
               </configOption>
               <configOption name="enable_idempotence">
                   <synopsis>Enable idempotent producer</synopsis>
                   <description>
                       <para>When enabled, the producer ensures exactly-once delivery
                       semantics per partition. Defaults to no.</para>
                   </description>
               </configOption>
               <configOption name="reconnect_backoff_ms">
                   <synopsis>Initial reconnect backoff in milliseconds</synopsis>
                   <description>
                       <para>Initial time to wait before reconnecting to a broker after
                       a disconnect. Defaults to 100.</para>
                   </description>
               </configOption>
               <configOption name="reconnect_backoff_max_ms">
                   <synopsis>Maximum reconnect backoff in milliseconds</synopsis>
                   <description>
                       <para>Maximum time to wait before reconnecting to a broker
                       (exponential backoff ceiling). Defaults to 10000.</para>
                   </description>
               </configOption>
               <configOption name="debug">
                   <synopsis>librdkafka debug contexts</synopsis>
                   <description>
                       <para>Comma-separated list of librdkafka debug contexts
                       (e.g. <literal>broker,topic,msg</literal>). Empty by default.</para>
                   </description>
               </configOption>
               <configOption name="group_id">
                   <synopsis>Consumer group ID</synopsis>
                   <description>
                       <para>Consumer group identifier. Required for consumer mode.
                       Empty by default (consumer disabled for this connection).</para>
                   </description>
               </configOption>
               <configOption name="auto_offset_reset">
                   <synopsis>Consumer auto offset reset policy</synopsis>
                   <description>
                       <para>Action to take when there is no initial offset or if the
                       current offset no longer exists: <literal>earliest</literal>,
                       <literal>latest</literal>, or <literal>none</literal>.
                       Defaults to <literal>latest</literal>.</para>
                   </description>
               </configOption>
               <configOption name="enable_auto_commit">
                   <synopsis>Enable automatic offset commit</synopsis>
                   <description>
                       <para>Automatically commit offsets periodically.
                       Defaults to yes.</para>
                   </description>
               </configOption>
               <configOption name="auto_commit_interval_ms">
                   <synopsis>Auto commit interval in milliseconds</synopsis>
                   <description>
                       <para>How often to auto-commit consumer offsets.
                       Defaults to 5000.</para>
                   </description>
               </configOption>
               <configOption name="session_timeout_ms">
                   <synopsis>Consumer session timeout in milliseconds</synopsis>
                   <description>
                       <para>Client group session and failure detection timeout.
                       If no heartbeat is received by the broker within this time,
                       the consumer is removed from the group. Defaults to 45000.</para>
                   </description>
               </configOption>
               <configOption name="max_poll_interval_ms">
                   <synopsis>Maximum consumer poll interval in milliseconds</synopsis>
                   <description>
                       <para>Maximum time between poll calls before the consumer is
                       considered failed. Defaults to 300000.</para>
                   </description>
               </configOption>
           </configObject>
       </configFile>
   </configInfo>
 ***/


#include "asterisk.h"

#include "asterisk/module.h"
#include "asterisk/kafka.h"
#include "kafka/internal.h"

#include <librdkafka/rdkafka.h>


#define NUM_ACTIVE_PRODUCER_BUCKETS 31
#define NUM_ACTIVE_CONSUMER_BUCKETS 31

static struct ao2_container *active_producers;
static struct ao2_container *active_consumers;

static pthread_t poll_thread_id = AST_PTHREADT_NULL;
static int poll_thread_run;

struct ast_kafka_producer
{
	rd_kafka_t *rk;
	char name[];
};

struct ast_kafka_consumer
{
	rd_kafka_t *rk;
	ast_kafka_message_cb callback;
	void *userdata;
	int subscribed;
	char name[];
};

/* ---- Producer hash/cmp/dtor ---- */

static int kafka_producer_hash(const void *obj, int flags)
{
	const struct ast_kafka_producer *producer = obj;
	const char *key;

	switch (flags & OBJ_SEARCH_MASK) {
	case OBJ_SEARCH_KEY:
		key = obj;
		break;
	case OBJ_SEARCH_OBJECT:
		producer = obj;
		key = producer->name;
		break;
	default:
		/* Hash can only work on something with a full key. */
		ast_assert(0);
		return 0;
	}

	return ast_str_hash(key);
}

static int kafka_producer_cmp(void *obj_left, void *arg, int flags)
{
	const struct ast_kafka_producer *producer_left = obj_left;
	const struct ast_kafka_producer *producer_right = arg;
	const char *right_key = arg;
	int cmp;

	switch (flags & OBJ_SEARCH_MASK) {
	case OBJ_SEARCH_OBJECT:
		right_key = producer_right->name;
		/* Fall through */
	case OBJ_SEARCH_KEY:
		cmp = strcmp(producer_left->name, right_key);
		break;
	case OBJ_SEARCH_PARTIAL_KEY:
		cmp = strncmp(producer_left->name, right_key, strlen(right_key));
		break;
	default:
		cmp = 0;
		break;
	}

	if (cmp) {
		return 0;
	}

	return CMP_MATCH;
}

static void kafka_producer_dtor(void *obj)
{
	struct ast_kafka_producer *producer = obj;
	ast_debug(3, "Destroying Kafka producer %s\n", producer->name);
	if (producer->rk) {
		rd_kafka_flush(producer->rk, 5000);
		rd_kafka_destroy(producer->rk);
		producer->rk = NULL;
	}
}

/* ---- Consumer hash/cmp/dtor ---- */

static int kafka_consumer_hash(const void *obj, int flags)
{
	const struct ast_kafka_consumer *consumer = obj;
	const char *key;

	switch (flags & OBJ_SEARCH_MASK) {
	case OBJ_SEARCH_KEY:
		key = obj;
		break;
	case OBJ_SEARCH_OBJECT:
		consumer = obj;
		key = consumer->name;
		break;
	default:
		ast_assert(0);
		return 0;
	}

	return ast_str_hash(key);
}

static int kafka_consumer_cmp(void *obj_left, void *arg, int flags)
{
	const struct ast_kafka_consumer *consumer_left = obj_left;
	const struct ast_kafka_consumer *consumer_right = arg;
	const char *right_key = arg;
	int cmp;

	switch (flags & OBJ_SEARCH_MASK) {
	case OBJ_SEARCH_OBJECT:
		right_key = consumer_right->name;
		/* Fall through */
	case OBJ_SEARCH_KEY:
		cmp = strcmp(consumer_left->name, right_key);
		break;
	case OBJ_SEARCH_PARTIAL_KEY:
		cmp = strncmp(consumer_left->name, right_key, strlen(right_key));
		break;
	default:
		cmp = 0;
		break;
	}

	if (cmp) {
		return 0;
	}

	return CMP_MATCH;
}

static void kafka_consumer_dtor(void *obj)
{
	struct ast_kafka_consumer *consumer = obj;
	ast_debug(3, "Destroying Kafka consumer %s\n", consumer->name);
	if (consumer->rk) {
		rd_kafka_consumer_close(consumer->rk);
		rd_kafka_destroy(consumer->rk);
		consumer->rk = NULL;
	}
}

/* ---- Shared callbacks ---- */

static void kafka_log_callback(const rd_kafka_t *rk, int level,
	const char *fac, const char *buf)
{
	/* librdkafka passes syslog levels: EMERG=0, ALERT=1, CRIT=2, ERR=3,
	 * WARNING=4, NOTICE=5, INFO=6, DEBUG=7.
	 * We cannot use <syslog.h> constants here because Asterisk's logger.h
	 * redefines LOG_WARNING, LOG_NOTICE, etc. as its own macros. */
	switch (level) {
	case 0: /* LOG_EMERG */
	case 1: /* LOG_ALERT */
	case 2: /* LOG_CRIT */
	case 3: /* LOG_ERR */
		ast_log(LOG_ERROR, "rdkafka [%s]: %s: %s\n",
			rd_kafka_name(rk), fac, buf);
		break;
	case 4: /* LOG_WARNING */
	case 5: /* LOG_NOTICE */
		ast_log(LOG_WARNING, "rdkafka [%s]: %s: %s\n",
			rd_kafka_name(rk), fac, buf);
		break;
	default: /* LOG_INFO=6, LOG_DEBUG=7 */
		ast_debug(3, "rdkafka [%s]: %s: %s\n",
			rd_kafka_name(rk), fac, buf);
		break;
	}
}

static void kafka_dr_msg_callback(rd_kafka_t *rk,
	const rd_kafka_message_t *rkmessage, void *opaque)
{
	if (rkmessage->err) {
		ast_log(LOG_ERROR, "Kafka delivery failed: %s\n",
			rd_kafka_err2str(rkmessage->err));
	} else {
		ast_debug(3, "Kafka message delivered (%zd bytes, partition %d)\n",
			rkmessage->len, rkmessage->partition);
	}
}

static void kafka_error_callback(rd_kafka_t *rk, int err,
	const char *reason, void *opaque)
{
	if (err == RD_KAFKA_RESP_ERR__FATAL) {
		char fatal_errstr[256];
		rd_kafka_resp_err_t fatal_err = rd_kafka_fatal_error(rk, fatal_errstr, sizeof(fatal_errstr));
		ast_log(LOG_ERROR, "rdkafka FATAL ERROR [%s]: %s (%s) - instance must be restarted\n",
			rd_kafka_name(rk), rd_kafka_err2str(fatal_err), fatal_errstr);
	} else {
		ast_log(LOG_WARNING, "rdkafka error [%s]: %s: %s\n",
			rd_kafka_name(rk), rd_kafka_err2str(err), reason);
	}
}

static void kafka_rebalance_callback(rd_kafka_t *rk,
	rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions,
	void *opaque)
{
	switch (err) {
	case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
		ast_debug(1, "rdkafka [%s]: partition assignment (%d partitions)\n",
			rd_kafka_name(rk), partitions->cnt);
		rd_kafka_assign(rk, partitions);
		break;
	case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		ast_debug(1, "rdkafka [%s]: partition revocation (%d partitions)\n",
			rd_kafka_name(rk), partitions->cnt);
		rd_kafka_assign(rk, NULL);
		break;
	default:
		ast_log(LOG_ERROR, "rdkafka [%s]: rebalance error: %s\n",
			rd_kafka_name(rk), rd_kafka_err2str(err));
		rd_kafka_assign(rk, NULL);
		break;
	}
}

/* ---- Poll thread ---- */

static void *kafka_poll_thread(void *data)
{
	while (poll_thread_run) {
		struct ao2_iterator i;
		struct ast_kafka_producer *producer;
		struct ast_kafka_consumer *consumer;

		/* Poll producers */
		i = ao2_iterator_init(active_producers, 0);
		while ((producer = ao2_iterator_next(&i))) {
			if (producer->rk) {
				rd_kafka_poll(producer->rk, 0);
			}
			ao2_ref(producer, -1);
		}
		ao2_iterator_destroy(&i);

		/* Poll consumers */
		i = ao2_iterator_init(active_consumers, 0);
		while ((consumer = ao2_iterator_next(&i))) {
			if (consumer->rk && consumer->subscribed && consumer->callback) {
				rd_kafka_message_t *rkmsg;
				while ((rkmsg = rd_kafka_consumer_poll(consumer->rk, 0)) != NULL) {
					if (rkmsg->err) {
						if (rkmsg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
							ast_log(LOG_WARNING, "Kafka consumer [%s] error: %s\n",
								consumer->name, rd_kafka_message_errstr(rkmsg));
						}
					} else {
						consumer->callback(
							rd_kafka_topic_name(rkmsg->rkt),
							rkmsg->partition,
							rkmsg->offset,
							rkmsg->payload,
							rkmsg->len,
							rkmsg->key,
							rkmsg->key_len,
							consumer->userdata);
					}
					rd_kafka_message_destroy(rkmsg);
				}
			}
			ao2_ref(consumer, -1);
		}
		ao2_iterator_destroy(&i);

		/* Sleep 100ms between poll cycles */
		usleep(100000);
	}

	return NULL;
}

/* ---- Config set helpers ---- */

/*! \brief Helper macro to set a librdkafka config option, jumping to conf_error on failure */
#define KAFKA_CONF_SET(conf, key, val) do { \
	if (rd_kafka_conf_set(conf, key, val, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) { \
		ast_log(LOG_ERROR, "Kafka config error (%s): %s\n", key, errstr); \
		goto conf_error; \
	} \
} while (0)

/*! \brief Helper macro to set a librdkafka config option from an int value */
#define KAFKA_CONF_SET_INT(conf, key, intval) do { \
	snprintf(value_str, sizeof(value_str), "%d", intval); \
	KAFKA_CONF_SET(conf, key, value_str); \
} while (0)

/*! \brief Apply shared connection config (security, network) to a librdkafka conf object */
static int kafka_conf_apply_shared(rd_kafka_conf_t *conf,
	struct kafka_conf_connection *cxn_conf)
{
	char errstr[512];
	char value_str[64];

	KAFKA_CONF_SET(conf, "bootstrap.servers", cxn_conf->brokers);
	KAFKA_CONF_SET(conf, "client.id", cxn_conf->client_id);
	KAFKA_CONF_SET_INT(conf, "message.max.bytes", cxn_conf->message_max_bytes);
	KAFKA_CONF_SET_INT(conf, "request.timeout.ms", cxn_conf->request_timeout_ms);

	/* Security / Authentication */
	KAFKA_CONF_SET(conf, "security.protocol", cxn_conf->security_protocol);
	if (!ast_strlen_zero(cxn_conf->sasl_mechanisms)) {
		KAFKA_CONF_SET(conf, "sasl.mechanisms", cxn_conf->sasl_mechanisms);
	}
	if (!ast_strlen_zero(cxn_conf->sasl_username)) {
		KAFKA_CONF_SET(conf, "sasl.username", cxn_conf->sasl_username);
	}
	if (!ast_strlen_zero(cxn_conf->sasl_password)) {
		KAFKA_CONF_SET(conf, "sasl.password", cxn_conf->sasl_password);
	}
	if (!ast_strlen_zero(cxn_conf->ssl_ca_location)) {
		KAFKA_CONF_SET(conf, "ssl.ca.location", cxn_conf->ssl_ca_location);
	}
	if (!ast_strlen_zero(cxn_conf->ssl_certificate_location)) {
		KAFKA_CONF_SET(conf, "ssl.certificate.location", cxn_conf->ssl_certificate_location);
	}
	if (!ast_strlen_zero(cxn_conf->ssl_key_location)) {
		KAFKA_CONF_SET(conf, "ssl.key.location", cxn_conf->ssl_key_location);
	}

	/* Network / Reconnection */
	KAFKA_CONF_SET_INT(conf, "reconnect.backoff.ms", cxn_conf->reconnect_backoff_ms);
	KAFKA_CONF_SET_INT(conf, "reconnect.backoff.max.ms", cxn_conf->reconnect_backoff_max_ms);

	/* Debug */
	if (!ast_strlen_zero(cxn_conf->debug)) {
		KAFKA_CONF_SET(conf, "debug", cxn_conf->debug);
	}

	rd_kafka_conf_set_log_cb(conf, kafka_log_callback);
	rd_kafka_conf_set_error_cb(conf, kafka_error_callback);

	return 0;

conf_error:
	return -1;
}

/* ---- Producer creation ---- */

static struct ast_kafka_producer *kafka_producer_create(
	const char *name)
{
	struct ast_kafka_producer *producer = NULL;
	RAII_VAR(struct kafka_conf_connection *, cxn_conf, NULL, ao2_cleanup);
	rd_kafka_conf_t *conf;
	char errstr[512];
	char value_str[64];

	ast_debug(3, "Creating Kafka producer %s\n", name);

	cxn_conf = kafka_config_get_connection(name);
	if (!cxn_conf) {
		ast_log(LOG_WARNING, "No Kafka config for connection '%s'\n", name);
		return NULL;
	}

	producer = ao2_alloc(sizeof(*producer) + strlen(name) + 1, kafka_producer_dtor);
	if (!producer) {
		ast_log(LOG_ERROR, "Allocation failed\n");
		return NULL;
	}

	strcpy(producer->name, name); /* SAFE */

	conf = rd_kafka_conf_new();
	if (!conf) {
		ast_log(LOG_ERROR, "Failed to create Kafka configuration\n");
		ao2_cleanup(producer);
		return NULL;
	}

	if (kafka_conf_apply_shared(conf, cxn_conf) != 0) {
		goto conf_error;
	}

	/* Producer-specific config */
	KAFKA_CONF_SET_INT(conf, "message.timeout.ms", cxn_conf->message_timeout_ms);
	KAFKA_CONF_SET(conf, "compression.codec", cxn_conf->compression_codec);
	KAFKA_CONF_SET_INT(conf, "compression.level", cxn_conf->compression_level);
	KAFKA_CONF_SET_INT(conf, "queue.buffering.max.ms", cxn_conf->linger_ms);
	KAFKA_CONF_SET_INT(conf, "batch.num.messages", cxn_conf->batch_num_messages);
	KAFKA_CONF_SET_INT(conf, "batch.size", cxn_conf->batch_size);
	KAFKA_CONF_SET_INT(conf, "queue.buffering.max.messages", cxn_conf->queue_buffering_max_messages);
	KAFKA_CONF_SET_INT(conf, "queue.buffering.max.kbytes", cxn_conf->queue_buffering_max_kbytes);
	KAFKA_CONF_SET_INT(conf, "request.required.acks", cxn_conf->acks);
	KAFKA_CONF_SET_INT(conf, "message.send.max.retries", cxn_conf->retries);
	KAFKA_CONF_SET(conf, "enable.idempotence", cxn_conf->enable_idempotence ? "true" : "false");

	rd_kafka_conf_set_dr_msg_cb(conf, kafka_dr_msg_callback);

	/* rd_kafka_new() takes ownership of conf on success */
	producer->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!producer->rk) {
		ast_log(LOG_ERROR, "Failed to create Kafka producer: %s\n", errstr);
		/* conf is destroyed by rd_kafka_new on failure */
		ao2_cleanup(producer);
		return NULL;
	}

	return producer;

conf_error:
	rd_kafka_conf_destroy(conf);
	ao2_cleanup(producer);
	return NULL;
}

/* ---- Consumer creation ---- */

static struct ast_kafka_consumer *kafka_consumer_create(
	const char *name)
{
	struct ast_kafka_consumer *consumer = NULL;
	RAII_VAR(struct kafka_conf_connection *, cxn_conf, NULL, ao2_cleanup);
	rd_kafka_conf_t *conf;
	char errstr[512];
	char value_str[64];

	ast_debug(3, "Creating Kafka consumer %s\n", name);

	cxn_conf = kafka_config_get_connection(name);
	if (!cxn_conf) {
		ast_log(LOG_WARNING, "No Kafka config for connection '%s'\n", name);
		return NULL;
	}

	if (ast_strlen_zero(cxn_conf->group_id)) {
		ast_log(LOG_ERROR, "Kafka connection '%s': group_id is required for consumer\n", name);
		return NULL;
	}

	consumer = ao2_alloc(sizeof(*consumer) + strlen(name) + 1, kafka_consumer_dtor);
	if (!consumer) {
		ast_log(LOG_ERROR, "Allocation failed\n");
		return NULL;
	}

	strcpy(consumer->name, name); /* SAFE */
	consumer->callback = NULL;
	consumer->userdata = NULL;
	consumer->subscribed = 0;

	conf = rd_kafka_conf_new();
	if (!conf) {
		ast_log(LOG_ERROR, "Failed to create Kafka configuration\n");
		ao2_cleanup(consumer);
		return NULL;
	}

	if (kafka_conf_apply_shared(conf, cxn_conf) != 0) {
		goto conf_error;
	}

	/* Consumer-specific config */
	KAFKA_CONF_SET(conf, "group.id", cxn_conf->group_id);
	KAFKA_CONF_SET(conf, "auto.offset.reset", cxn_conf->auto_offset_reset);
	KAFKA_CONF_SET(conf, "enable.auto.commit", cxn_conf->enable_auto_commit ? "true" : "false");
	KAFKA_CONF_SET_INT(conf, "auto.commit.interval.ms", cxn_conf->auto_commit_interval_ms);
	KAFKA_CONF_SET_INT(conf, "session.timeout.ms", cxn_conf->session_timeout_ms);
	KAFKA_CONF_SET_INT(conf, "max.poll.interval.ms", cxn_conf->max_poll_interval_ms);

	rd_kafka_conf_set_rebalance_cb(conf, kafka_rebalance_callback);

	/* rd_kafka_new() takes ownership of conf on success */
	consumer->rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!consumer->rk) {
		ast_log(LOG_ERROR, "Failed to create Kafka consumer: %s\n", errstr);
		/* conf is destroyed by rd_kafka_new on failure */
		ao2_cleanup(consumer);
		return NULL;
	}

	/* Forward partition messages to the consumer queue for consumer_poll() */
	rd_kafka_poll_set_consumer(consumer->rk);

	return consumer;

conf_error:
	rd_kafka_conf_destroy(conf);
	ao2_cleanup(consumer);
	return NULL;
}

#undef KAFKA_CONF_SET
#undef KAFKA_CONF_SET_INT

/* ---- Public producer API ---- */

struct ast_kafka_producer *ast_kafka_get_producer(const char *name)
{
	SCOPED_AO2LOCK(producers_lock, active_producers);
	struct ast_kafka_producer *producer =
		ao2_find(active_producers, name, OBJ_SEARCH_KEY | OBJ_NOLOCK);

	if (!producer) {
		producer = kafka_producer_create(name);

		if (!producer) {
			return NULL;
		}

		if (!ao2_link_flags(active_producers, producer, OBJ_NOLOCK)) {
			ast_log(LOG_ERROR, "Allocation failed\n");
			ao2_cleanup(producer);
			return NULL;
		}
	}

	return producer;
}

int ast_kafka_produce_hdrs(struct ast_kafka_producer *producer,
	const char *topic,
	const char *key,
	const void *payload,
	size_t len,
	const struct ast_kafka_header *headers,
	size_t header_count)
{
	rd_kafka_resp_err_t err;
	rd_kafka_headers_t *hdrs = NULL;

	if (!producer || !producer->rk) {
		return -1;
	}

	if (headers && header_count > 0) {
		size_t i;
		hdrs = rd_kafka_headers_new((int) header_count);
		if (!hdrs) {
			ast_log(LOG_ERROR, "Failed to allocate Kafka headers\n");
			return -1;
		}
		for (i = 0; i < header_count; i++) {
			rd_kafka_header_add(hdrs,
				headers[i].name, -1,
				headers[i].value, -1);
		}
	}

	err = rd_kafka_producev(
		producer->rk,
		RD_KAFKA_V_TOPIC(topic),
		RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
		RD_KAFKA_V_VALUE((void *)payload, len),
		RD_KAFKA_V_KEY(key, key ? strlen(key) : 0),
		RD_KAFKA_V_HEADERS(hdrs),
		RD_KAFKA_V_END);

	if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
		/* Internal queue full -- flush pending messages and retry once.
		 * On failure, rd_kafka_producev does NOT take ownership of headers,
		 * so we can safely reuse them for the retry. */
		ast_log(LOG_WARNING, "Kafka producer queue full for topic '%s', flushing...\n", topic);
		rd_kafka_poll(producer->rk, 100);

		/* Rebuild headers since librdkafka took ownership on the first attempt
		 * only if it succeeded (which it didn't — queue full). However,
		 * rd_kafka_producev does NOT destroy headers on QUEUE_FULL, so hdrs
		 * is still valid. But to be safe with other error codes in the future,
		 * we pass the same hdrs pointer. */
		err = rd_kafka_producev(
			producer->rk,
			RD_KAFKA_V_TOPIC(topic),
			RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
			RD_KAFKA_V_VALUE((void *)payload, len),
			RD_KAFKA_V_KEY(key, key ? strlen(key) : 0),
			RD_KAFKA_V_HEADERS(hdrs),
			RD_KAFKA_V_END);
	}

	if (err) {
		/* On error, librdkafka does NOT take ownership of headers */
		if (hdrs) {
			rd_kafka_headers_destroy(hdrs);
		}
		ast_log(LOG_ERROR, "Error producing to Kafka topic '%s': %s\n",
			topic, rd_kafka_err2str(err));
		return -1;
	}

	/* On success, librdkafka takes ownership of hdrs — do not destroy */
	return 0;
}

int ast_kafka_produce(struct ast_kafka_producer *producer,
	const char *topic,
	const char *key,
	const void *payload,
	size_t len)
{
	return ast_kafka_produce_hdrs(producer, topic, key, payload, len, NULL, 0);
}

/* ---- Public consumer API ---- */

struct ast_kafka_consumer *ast_kafka_get_consumer(const char *name)
{
	SCOPED_AO2LOCK(consumers_lock, active_consumers);
	struct ast_kafka_consumer *consumer =
		ao2_find(active_consumers, name, OBJ_SEARCH_KEY | OBJ_NOLOCK);

	if (!consumer) {
		consumer = kafka_consumer_create(name);

		if (!consumer) {
			return NULL;
		}

		if (!ao2_link_flags(active_consumers, consumer, OBJ_NOLOCK)) {
			ast_log(LOG_ERROR, "Allocation failed\n");
			ao2_cleanup(consumer);
			return NULL;
		}
	}

	return consumer;
}

int ast_kafka_consumer_subscribe(struct ast_kafka_consumer *consumer,
	const char *topics,
	ast_kafka_message_cb callback,
	void *userdata)
{
	rd_kafka_topic_partition_list_t *tpl;
	rd_kafka_resp_err_t err;
	char *topics_copy;
	char *topic;
	char *saveptr = NULL;

	if (!consumer || !consumer->rk || !topics || !callback) {
		return -1;
	}

	topics_copy = ast_strdupa(topics);

	tpl = rd_kafka_topic_partition_list_new(8);
	if (!tpl) {
		ast_log(LOG_ERROR, "Failed to allocate topic partition list\n");
		return -1;
	}

	for (topic = strtok_r(topics_copy, ",", &saveptr);
	     topic;
	     topic = strtok_r(NULL, ",", &saveptr)) {
		/* Trim leading/trailing whitespace */
		while (*topic == ' ') topic++;
		char *end = topic + strlen(topic) - 1;
		while (end > topic && *end == ' ') *end-- = '\0';

		if (!ast_strlen_zero(topic)) {
			rd_kafka_topic_partition_list_add(tpl, topic, RD_KAFKA_PARTITION_UA);
			ast_debug(3, "Kafka consumer [%s] subscribing to topic: %s\n",
				consumer->name, topic);
		}
	}

	if (tpl->cnt == 0) {
		ast_log(LOG_ERROR, "Kafka consumer [%s]: no valid topics specified\n",
			consumer->name);
		rd_kafka_topic_partition_list_destroy(tpl);
		return -1;
	}

	err = rd_kafka_subscribe(consumer->rk, tpl);
	rd_kafka_topic_partition_list_destroy(tpl);

	if (err) {
		ast_log(LOG_ERROR, "Kafka consumer [%s] subscribe failed: %s\n",
			consumer->name, rd_kafka_err2str(err));
		return -1;
	}

	consumer->callback = callback;
	consumer->userdata = userdata;
	consumer->subscribed = 1;

	ast_debug(1, "Kafka consumer [%s] subscribed successfully\n", consumer->name);

	return 0;
}

int ast_kafka_consumer_unsubscribe(struct ast_kafka_consumer *consumer)
{
	rd_kafka_resp_err_t err;

	if (!consumer || !consumer->rk) {
		return -1;
	}

	if (!consumer->subscribed) {
		return 0;
	}

	err = rd_kafka_unsubscribe(consumer->rk);
	if (err) {
		ast_log(LOG_ERROR, "Kafka consumer [%s] unsubscribe failed: %s\n",
			consumer->name, rd_kafka_err2str(err));
		return -1;
	}

	consumer->subscribed = 0;
	consumer->callback = NULL;
	consumer->userdata = NULL;

	ast_debug(1, "Kafka consumer [%s] unsubscribed\n", consumer->name);

	return 0;
}

/* ---- Accessor functions for CLI ---- */

void kafka_foreach_consumer(kafka_consumer_foreach_cb cb, void *arg)
{
	struct ao2_iterator i;
	struct ast_kafka_consumer *consumer;

	i = ao2_iterator_init(active_consumers, 0);
	while ((consumer = ao2_iterator_next(&i))) {
		int res = cb(consumer, arg);
		ao2_ref(consumer, -1);
		if (res) {
			break;
		}
	}
	ao2_iterator_destroy(&i);
}

const char *kafka_consumer_get_name(struct ast_kafka_consumer *consumer)
{
	return consumer->name;
}

int kafka_consumer_is_subscribed(struct ast_kafka_consumer *consumer)
{
	return consumer->subscribed;
}

/* ---- Admin API ---- */

int ast_kafka_ensure_topic(struct ast_kafka_producer *producer,
	const char *topic, int num_partitions, int replication_factor)
{
	rd_kafka_NewTopic_t *new_topic;
	rd_kafka_NewTopic_t *topics[1];
	rd_kafka_AdminOptions_t *options;
	rd_kafka_queue_t *queue;
	rd_kafka_event_t *event;
	char errstr[256];
	int res = -1;

	if (!producer || !producer->rk || ast_strlen_zero(topic)) {
		return -1;
	}

	new_topic = rd_kafka_NewTopic_new(topic, num_partitions,
		replication_factor, errstr, sizeof(errstr));
	if (!new_topic) {
		ast_log(LOG_ERROR, "Failed to create NewTopic object for '%s': %s\n",
			topic, errstr);
		return -1;
	}

	topics[0] = new_topic;

	options = rd_kafka_AdminOptions_new(producer->rk,
		RD_KAFKA_ADMIN_OP_CREATETOPICS);
	if (rd_kafka_AdminOptions_set_request_timeout(options, 10000,
		errstr, sizeof(errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR) {
		ast_log(LOG_WARNING, "Failed to set admin timeout: %s\n", errstr);
	}

	queue = rd_kafka_queue_new(producer->rk);

	rd_kafka_CreateTopics(producer->rk, topics, 1, options, queue);

	/* Wait for result */
	event = rd_kafka_queue_poll(queue, 15000);
	if (!event) {
		ast_log(LOG_ERROR, "Timeout waiting for CreateTopics result "
			"for '%s'\n", topic);
		goto cleanup;
	}

	if (rd_kafka_event_type(event) == RD_KAFKA_EVENT_CREATETOPICS_RESULT) {
		const rd_kafka_CreateTopics_result_t *result =
			rd_kafka_event_CreateTopics_result(event);
		size_t cnt;
		const rd_kafka_topic_result_t **topic_results =
			rd_kafka_CreateTopics_result_topics(result, &cnt);

		if (cnt > 0) {
			rd_kafka_resp_err_t err =
				rd_kafka_topic_result_error(topic_results[0]);
			if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
				ast_debug(1, "Kafka topic '%s' created "
					"(%d partitions, rf %d)\n",
					topic, num_partitions,
					replication_factor);
				res = 0;
			} else if (err == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
				ast_debug(1, "Kafka topic '%s' already exists\n",
					topic);
				res = 0;
			} else {
				ast_log(LOG_ERROR, "Failed to create topic '%s': "
					"%s: %s\n", topic,
					rd_kafka_err2str(err),
					rd_kafka_topic_result_error_string(
						topic_results[0]));
			}
		}
	} else {
		ast_log(LOG_ERROR, "Unexpected event type %d for CreateTopics\n",
			rd_kafka_event_type(event));
	}

	rd_kafka_event_destroy(event);

cleanup:
	rd_kafka_queue_destroy(queue);
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_NewTopic_destroy(new_topic);

	return res;
}

/* ---- Module lifecycle ---- */

static int load_module(void)
{
	ast_debug(3, "Loading Kafka module (librdkafka %s)\n",
		rd_kafka_version_str());

	if (kafka_config_init() != 0) {
		ast_log(LOG_ERROR, "Failed to init Kafka config\n");
		return AST_MODULE_LOAD_DECLINE;
	}

	active_producers = ao2_container_alloc_hash(AO2_ALLOC_OPT_LOCK_MUTEX, 0, NUM_ACTIVE_PRODUCER_BUCKETS,
		kafka_producer_hash, NULL, kafka_producer_cmp);
	if (!active_producers) {
		ast_log(LOG_ERROR, "Allocation failure\n");
		return AST_MODULE_LOAD_FAILURE;
	}

	active_consumers = ao2_container_alloc_hash(AO2_ALLOC_OPT_LOCK_MUTEX, 0, NUM_ACTIVE_CONSUMER_BUCKETS,
		kafka_consumer_hash, NULL, kafka_consumer_cmp);
	if (!active_consumers) {
		ast_log(LOG_ERROR, "Allocation failure\n");
		ao2_cleanup(active_producers);
		return AST_MODULE_LOAD_FAILURE;
	}

	poll_thread_run = 1;
	if (ast_pthread_create_background(&poll_thread_id, NULL, kafka_poll_thread, NULL) < 0) {
		ast_log(LOG_ERROR, "Failed to create Kafka poll thread\n");
		ao2_cleanup(active_consumers);
		ao2_cleanup(active_producers);
		return AST_MODULE_LOAD_FAILURE;
	}

	if (kafka_cli_register() != 0) {
		ast_log(LOG_ERROR, "Failed to register Kafka CLI\n");
		poll_thread_run = 0;
		pthread_join(poll_thread_id, NULL);
		ao2_cleanup(active_consumers);
		ao2_cleanup(active_producers);
		return AST_MODULE_LOAD_FAILURE;
	}

	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void)
{
	poll_thread_run = 0;
	if (poll_thread_id != AST_PTHREADT_NULL) {
		pthread_join(poll_thread_id, NULL);
		poll_thread_id = AST_PTHREADT_NULL;
	}

	kafka_cli_unregister();
	ao2_cleanup(active_consumers);
	ao2_cleanup(active_producers);
	kafka_config_destroy();
	return 0;
}

static int reload_module(void)
{
	if (kafka_config_reload() != 0) {
		return AST_MODULE_LOAD_DECLINE;
	}

	return AST_MODULE_LOAD_SUCCESS;
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_GLOBAL_SYMBOLS | AST_MODFLAG_LOAD_ORDER, "Kafka Producer and Consumer Interface",
	.support_level = AST_MODULE_SUPPORT_CORE,
	.load = load_module,
	.unload = unload_module,
	.reload = reload_module,
	.load_pri = AST_MODPRI_APP_DEPEND,
	);
