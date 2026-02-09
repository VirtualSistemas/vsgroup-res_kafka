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
 * \brief Kafka producer APIs.
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
       <synopsis>Kafka producer API</synopsis>
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

static struct ao2_container *active_producers;

static pthread_t poll_thread_id = AST_PTHREADT_NULL;
static int poll_thread_run;

struct ast_kafka_producer
{
	rd_kafka_t *rk;
	char name[];
};

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
		ast_log(LOG_ERROR, "rdkafka FATAL ERROR [%s]: %s (%s) - producer must be restarted\n",
			rd_kafka_name(rk), rd_kafka_err2str(fatal_err), fatal_errstr);
	} else {
		ast_log(LOG_WARNING, "rdkafka error [%s]: %s: %s\n",
			rd_kafka_name(rk), rd_kafka_err2str(err), reason);
	}
}

static void *kafka_poll_thread(void *data)
{
	while (poll_thread_run) {
		struct ao2_iterator i;
		struct ast_kafka_producer *producer;

		i = ao2_iterator_init(active_producers, 0);
		while ((producer = ao2_iterator_next(&i))) {
			if (producer->rk) {
				rd_kafka_poll(producer->rk, 0);
			}
			ao2_ref(producer, -1);
		}
		ao2_iterator_destroy(&i);

		/* Sleep 100ms between poll cycles */
		usleep(100000);
	}

	return NULL;
}

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

	if (rd_kafka_conf_set(conf, "bootstrap.servers", cxn_conf->brokers,
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		ast_log(LOG_ERROR, "Kafka config error: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		ao2_cleanup(producer);
		return NULL;
	}

	if (rd_kafka_conf_set(conf, "client.id", cxn_conf->client_id,
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		ast_log(LOG_ERROR, "Kafka config error: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		ao2_cleanup(producer);
		return NULL;
	}

	snprintf(value_str, sizeof(value_str), "%d", cxn_conf->message_max_bytes);
	if (rd_kafka_conf_set(conf, "message.max.bytes", value_str,
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		ast_log(LOG_ERROR, "Kafka config error: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		ao2_cleanup(producer);
		return NULL;
	}

	snprintf(value_str, sizeof(value_str), "%d", cxn_conf->request_timeout_ms);
	if (rd_kafka_conf_set(conf, "request.timeout.ms", value_str,
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		ast_log(LOG_ERROR, "Kafka config error: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		ao2_cleanup(producer);
		return NULL;
	}

	snprintf(value_str, sizeof(value_str), "%d", cxn_conf->message_timeout_ms);
	if (rd_kafka_conf_set(conf, "message.timeout.ms", value_str,
		errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		ast_log(LOG_ERROR, "Kafka config error: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		ao2_cleanup(producer);
		return NULL;
	}

	rd_kafka_conf_set_log_cb(conf, kafka_log_callback);
	rd_kafka_conf_set_dr_msg_cb(conf, kafka_dr_msg_callback);
	rd_kafka_conf_set_error_cb(conf, kafka_error_callback);

	/* rd_kafka_new() takes ownership of conf on success */
	producer->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!producer->rk) {
		ast_log(LOG_ERROR, "Failed to create Kafka producer: %s\n", errstr);
		/* conf is destroyed by rd_kafka_new on failure */
		ao2_cleanup(producer);
		return NULL;
	}

	return producer;
}

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

int ast_kafka_produce(struct ast_kafka_producer *producer,
	const char *topic,
	const char *key,
	const void *payload,
	size_t len)
{
	rd_kafka_resp_err_t err;

	if (!producer || !producer->rk) {
		return -1;
	}

	err = rd_kafka_producev(
		producer->rk,
		RD_KAFKA_V_TOPIC(topic),
		RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
		RD_KAFKA_V_VALUE((void *)payload, len),
		RD_KAFKA_V_KEY(key, key ? strlen(key) : 0),
		RD_KAFKA_V_END);

	if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
		/* Internal queue full — flush pending messages and retry once */
		ast_log(LOG_WARNING, "Kafka producer queue full for topic '%s', flushing...\n", topic);
		rd_kafka_poll(producer->rk, 100);

		err = rd_kafka_producev(
			producer->rk,
			RD_KAFKA_V_TOPIC(topic),
			RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
			RD_KAFKA_V_VALUE((void *)payload, len),
			RD_KAFKA_V_KEY(key, key ? strlen(key) : 0),
			RD_KAFKA_V_END);
	}

	if (err) {
		ast_log(LOG_ERROR, "Error producing to Kafka topic '%s': %s\n",
			topic, rd_kafka_err2str(err));
		return -1;
	}

	return 0;
}

static int load_module(void)
{
	ast_debug(3, "Loading Kafka producer module (librdkafka %s)\n",
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

	poll_thread_run = 1;
	if (ast_pthread_create_background(&poll_thread_id, NULL, kafka_poll_thread, NULL) < 0) {
		ast_log(LOG_ERROR, "Failed to create Kafka poll thread\n");
		ao2_cleanup(active_producers);
		return AST_MODULE_LOAD_FAILURE;
	}

	if (kafka_cli_register() != 0) {
		ast_log(LOG_ERROR, "Failed to register Kafka CLI\n");
		poll_thread_run = 0;
		pthread_join(poll_thread_id, NULL);
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

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_GLOBAL_SYMBOLS | AST_MODFLAG_LOAD_ORDER, "Kafka Producer Interface",
	.support_level = AST_MODULE_SUPPORT_CORE,
	.load = load_module,
	.unload = unload_module,
	.reload = reload_module,
	.load_pri = AST_MODPRI_APP_DEPEND,
	);
