/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright 2015-2024 The Wazo Authors  (see the AUTHORS file)
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
 * \brief Configuration for Kafka.
 */

#include "asterisk.h"


#include "asterisk/config_options.h"
#include "internal.h"

/*! \brief Locking container for safe configuration access. */
static AO2_GLOBAL_OBJ_STATIC(confs);

static struct aco_type general_option = {
	.type = ACO_GLOBAL,
	.name = "general",
	.item_offset = offsetof(struct kafka_conf, general),
	.category = "^general$",
	.category_match = ACO_WHITELIST,
};

static struct aco_type *general_options[] = ACO_TYPES(&general_option);

static void *kafka_conf_connection_alloc(const char *cat);
static void *kafka_conf_connection_find(struct ao2_container *tmp_container, const char *cat);

static struct aco_type connection_option = {
	.type = ACO_ITEM,
	.name = "connection",
	.category_match = ACO_BLACKLIST,
	.category = "^general$",
	.item_alloc = kafka_conf_connection_alloc,
	.item_find = kafka_conf_connection_find,
	.item_offset = offsetof(struct kafka_conf, connections),
};

static struct aco_type *connection_options[] = ACO_TYPES(&connection_option);

#define CONF_FILENAME "kafka.conf"

/*! \brief The conf file that's processed for the module. */
static struct aco_file conf_file = {
	/*! The config file name. */
	.filename = CONF_FILENAME,
	/*! The mapping object types to be processed. */
	.types = ACO_TYPES(&general_option, &connection_option),
};

static int kafka_conf_connection_sort_cmp(const void *obj_left, const void *obj_right, int flags);

static void conf_dtor(void *obj)
{
	struct kafka_conf *conf = obj;

	ao2_cleanup(conf->general);
	ao2_cleanup(conf->connections);
}

static void *conf_alloc(void)
{
	RAII_VAR(struct kafka_conf *, conf, NULL, ao2_cleanup);

	conf = ao2_alloc_options(sizeof(*conf), conf_dtor, AO2_ALLOC_OPT_LOCK_NOLOCK);
	if (!conf) {
		return NULL;
	}

	conf->general = ao2_alloc_options(sizeof(*conf->general), NULL,
		AO2_ALLOC_OPT_LOCK_NOLOCK);
	if (!conf->general) {
		return NULL;
	}
	aco_set_defaults(&general_option, "general", conf->general);

	conf->connections = ao2_container_alloc_rbtree(AO2_ALLOC_OPT_LOCK_NOLOCK,
		AO2_CONTAINER_ALLOC_OPT_DUPS_REPLACE, kafka_conf_connection_sort_cmp, NULL);
	if (!conf->connections) {
		return NULL;
	}

	return ao2_bump(conf);
}

static int validate_connections(void);

CONFIG_INFO_STANDARD(cfg_info, confs, conf_alloc,
	.files = ACO_FILES(&conf_file),
	.pre_apply_config = validate_connections,
);

static int validate_connection_cb(void *obj, void *arg, int flags)
{
	struct kafka_conf_connection *cxn_conf = obj;
	int *validation_res = arg;

	if (ast_strlen_zero(cxn_conf->brokers)) {
		ast_log(LOG_ERROR, "%s: brokers must not be empty\n",
			cxn_conf->name);
		*validation_res = -1;
		return -1;
	}

	if (cxn_conf->message_max_bytes < 1000) {
		ast_log(LOG_WARNING, "%s: invalid message_max_bytes %d, using 1000\n",
			cxn_conf->name, cxn_conf->message_max_bytes);
		cxn_conf->message_max_bytes = 1000;
	}

	if (cxn_conf->request_timeout_ms < 1) {
		ast_log(LOG_WARNING, "%s: invalid request_timeout_ms %d, using 5000\n",
			cxn_conf->name, cxn_conf->request_timeout_ms);
		cxn_conf->request_timeout_ms = 5000;
	}

	if (cxn_conf->message_timeout_ms < 1000) {
		ast_log(LOG_WARNING, "%s: invalid message_timeout_ms %d, using 300000\n",
			cxn_conf->name, cxn_conf->message_timeout_ms);
		cxn_conf->message_timeout_ms = 300000;
	}

	if (!ast_strlen_zero(cxn_conf->security_protocol) &&
		strcasecmp(cxn_conf->security_protocol, "plaintext") &&
		strcasecmp(cxn_conf->security_protocol, "ssl") &&
		strcasecmp(cxn_conf->security_protocol, "sasl_plaintext") &&
		strcasecmp(cxn_conf->security_protocol, "sasl_ssl")) {
		ast_log(LOG_ERROR, "%s: invalid security_protocol '%s' "
			"(must be plaintext, ssl, sasl_plaintext, or sasl_ssl)\n",
			cxn_conf->name, cxn_conf->security_protocol);
		*validation_res = -1;
		return -1;
	}

	if (!ast_strlen_zero(cxn_conf->compression_codec) &&
		strcasecmp(cxn_conf->compression_codec, "none") &&
		strcasecmp(cxn_conf->compression_codec, "gzip") &&
		strcasecmp(cxn_conf->compression_codec, "snappy") &&
		strcasecmp(cxn_conf->compression_codec, "lz4") &&
		strcasecmp(cxn_conf->compression_codec, "zstd")) {
		ast_log(LOG_ERROR, "%s: invalid compression_codec '%s' "
			"(must be none, gzip, snappy, lz4, or zstd)\n",
			cxn_conf->name, cxn_conf->compression_codec);
		*validation_res = -1;
		return -1;
	}

	if (cxn_conf->linger_ms < 0) {
		ast_log(LOG_WARNING, "%s: invalid linger_ms %d, using 5\n",
			cxn_conf->name, cxn_conf->linger_ms);
		cxn_conf->linger_ms = 5;
	}

	if (cxn_conf->batch_num_messages < 1) {
		ast_log(LOG_WARNING, "%s: invalid batch_num_messages %d, using 10000\n",
			cxn_conf->name, cxn_conf->batch_num_messages);
		cxn_conf->batch_num_messages = 10000;
	}

	if (cxn_conf->batch_size < 1) {
		ast_log(LOG_WARNING, "%s: invalid batch_size %d, using 1000000\n",
			cxn_conf->name, cxn_conf->batch_size);
		cxn_conf->batch_size = 1000000;
	}

	if (cxn_conf->queue_buffering_max_messages < 1) {
		ast_log(LOG_WARNING, "%s: invalid queue_buffering_max_messages %d, using 100000\n",
			cxn_conf->name, cxn_conf->queue_buffering_max_messages);
		cxn_conf->queue_buffering_max_messages = 100000;
	}

	if (cxn_conf->queue_buffering_max_kbytes < 1) {
		ast_log(LOG_WARNING, "%s: invalid queue_buffering_max_kbytes %d, using 1048576\n",
			cxn_conf->name, cxn_conf->queue_buffering_max_kbytes);
		cxn_conf->queue_buffering_max_kbytes = 1048576;
	}

	if (cxn_conf->acks < -1 || cxn_conf->acks > 1) {
		ast_log(LOG_WARNING, "%s: invalid acks %d (must be -1, 0, or 1), using -1\n",
			cxn_conf->name, cxn_conf->acks);
		cxn_conf->acks = -1;
	}

	if (cxn_conf->retries < 0) {
		ast_log(LOG_WARNING, "%s: invalid retries %d, using 2147483647\n",
			cxn_conf->name, cxn_conf->retries);
		cxn_conf->retries = 2147483647;
	}

	if (cxn_conf->reconnect_backoff_ms < 0) {
		ast_log(LOG_WARNING, "%s: invalid reconnect_backoff_ms %d, using 100\n",
			cxn_conf->name, cxn_conf->reconnect_backoff_ms);
		cxn_conf->reconnect_backoff_ms = 100;
	}

	if (cxn_conf->reconnect_backoff_max_ms < cxn_conf->reconnect_backoff_ms) {
		ast_log(LOG_WARNING, "%s: reconnect_backoff_max_ms %d < reconnect_backoff_ms %d, using %d\n",
			cxn_conf->name, cxn_conf->reconnect_backoff_max_ms,
			cxn_conf->reconnect_backoff_ms, cxn_conf->reconnect_backoff_ms);
		cxn_conf->reconnect_backoff_max_ms = cxn_conf->reconnect_backoff_ms;
	}

	return 0;
}

static int validate_connections(void)
{
	int validation_res = 0;

	struct kafka_conf *conf = aco_pending_config(&cfg_info);
	if (!conf) {
		ast_log(LOG_ERROR, "Error obtaining config from kafka.conf\n");
		return 0;
	}

	if (!conf->general->enabled) {
		ast_log(LOG_NOTICE, "Kafka disabled\n");
		return 0;
	}

	ast_debug(3, "Building %d Kafka connections\n",
	ao2_container_count(conf->connections));
	ao2_callback(conf->connections, OBJ_NODATA, validate_connection_cb, &validation_res);

	return validation_res;
}

static void kafka_conf_connection_dtor(void *obj)
{
	struct kafka_conf_connection *cxn_conf = obj;
	ast_debug(3, "Destroying Kafka connection config %s\n", cxn_conf->name);

	ast_string_field_free_memory(cxn_conf);
}

static void *kafka_conf_connection_alloc(const char *cat)
{
	RAII_VAR(struct kafka_conf_connection *, cxn_conf, NULL, ao2_cleanup);

	ast_debug(3, "Building Kafka connection %s\n", cat);

	if (!cat) {
		return NULL;
	}

	cxn_conf = ao2_alloc(sizeof(*cxn_conf), kafka_conf_connection_dtor);
	if (!cxn_conf) {
		return NULL;
	}

	if (ast_string_field_init(cxn_conf, 256) != 0) {
		return NULL;
	}

	if (ast_string_field_set(cxn_conf, name, cat) != 0) {
		return NULL;
	}

	ao2_ref(cxn_conf, +1);
	return cxn_conf;
}

static void *kafka_conf_connection_find(struct ao2_container *tmp_container, const char *cat)
{
	if (!cat) {
		return NULL;
	}

	return ao2_find(tmp_container, cat, OBJ_KEY);
}

int kafka_conf_connection_sort_cmp(const void *obj_left, const void *obj_right, int flags)
{
	const struct kafka_conf_connection *cxn_conf_left = obj_left;

	if (flags & OBJ_PARTIAL_KEY) {
		const char *key_right = obj_right;
		return strncasecmp(cxn_conf_left->name, key_right,
			strlen(key_right));
	} else if (flags & OBJ_KEY) {
		const char *key_right = obj_right;
		return strcasecmp(cxn_conf_left->name, key_right);
	} else {
		const struct kafka_conf_connection *cxn_conf_right = obj_right;
		const char *key_right = cxn_conf_right->name;
		return strcasecmp(cxn_conf_left->name, key_right);
	}
}

static int process_config(int reload)
{
	switch (aco_process_config(&cfg_info, reload)) {
	case ACO_PROCESS_ERROR:
		return -1;
	case ACO_PROCESS_OK:
	case ACO_PROCESS_UNCHANGED:
		break;
	}

	return 0;
}

int kafka_config_init(void)
{
	static char default_message_max_bytes_str[12];
	static char default_request_timeout_ms_str[12];
	static char default_message_timeout_ms_str[12];
	static char default_compression_level_str[12];
	static char default_linger_ms_str[12];
	static char default_batch_num_messages_str[12];
	static char default_batch_size_str[12];
	static char default_queue_buffering_max_messages_str[12];
	static char default_queue_buffering_max_kbytes_str[12];
	static char default_acks_str[12];
	static char default_retries_str[16];
	static char default_reconnect_backoff_ms_str[12];
	static char default_reconnect_backoff_max_ms_str[12];

	snprintf(default_message_max_bytes_str, sizeof(default_message_max_bytes_str),
		"%d", 1000000);
	snprintf(default_request_timeout_ms_str, sizeof(default_request_timeout_ms_str),
		"%d", 5000);
	snprintf(default_message_timeout_ms_str, sizeof(default_message_timeout_ms_str),
		"%d", 300000);
	snprintf(default_compression_level_str, sizeof(default_compression_level_str),
		"%d", -1);
	snprintf(default_linger_ms_str, sizeof(default_linger_ms_str),
		"%d", 5);
	snprintf(default_batch_num_messages_str, sizeof(default_batch_num_messages_str),
		"%d", 10000);
	snprintf(default_batch_size_str, sizeof(default_batch_size_str),
		"%d", 1000000);
	snprintf(default_queue_buffering_max_messages_str, sizeof(default_queue_buffering_max_messages_str),
		"%d", 100000);
	snprintf(default_queue_buffering_max_kbytes_str, sizeof(default_queue_buffering_max_kbytes_str),
		"%d", 1048576);
	snprintf(default_acks_str, sizeof(default_acks_str),
		"%d", -1);
	snprintf(default_retries_str, sizeof(default_retries_str),
		"%d", 2147483647);
	snprintf(default_reconnect_backoff_ms_str, sizeof(default_reconnect_backoff_ms_str),
		"%d", 100);
	snprintf(default_reconnect_backoff_max_ms_str, sizeof(default_reconnect_backoff_max_ms_str),
		"%d", 10000);

	if (aco_info_init(&cfg_info) != 0) {
		ast_log(LOG_ERROR, "Failed to initialize config\n");
		aco_info_destroy(&cfg_info);
		return -1;
	}

	aco_option_register(&cfg_info, "enabled", ACO_EXACT, general_options,
		"yes", OPT_BOOL_T, 1,
		FLDSET(struct kafka_conf_general, enabled));

	aco_option_register(&cfg_info, "type", ACO_EXACT, connection_options,
		NULL, OPT_NOOP_T, 0, 0);

	/* String fields */
	aco_option_register(&cfg_info, "brokers", ACO_EXACT,
		connection_options, "localhost:9092", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, brokers));
	aco_option_register(&cfg_info, "client_id", ACO_EXACT,
		connection_options, "asterisk", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, client_id));
	aco_option_register(&cfg_info, "security_protocol", ACO_EXACT,
		connection_options, "plaintext", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, security_protocol));
	aco_option_register(&cfg_info, "sasl_mechanisms", ACO_EXACT,
		connection_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, sasl_mechanisms));
	aco_option_register(&cfg_info, "sasl_username", ACO_EXACT,
		connection_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, sasl_username));
	aco_option_register(&cfg_info, "sasl_password", ACO_EXACT,
		connection_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, sasl_password));
	aco_option_register(&cfg_info, "ssl_ca_location", ACO_EXACT,
		connection_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, ssl_ca_location));
	aco_option_register(&cfg_info, "ssl_certificate_location", ACO_EXACT,
		connection_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, ssl_certificate_location));
	aco_option_register(&cfg_info, "ssl_key_location", ACO_EXACT,
		connection_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, ssl_key_location));
	aco_option_register(&cfg_info, "compression_codec", ACO_EXACT,
		connection_options, "none", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, compression_codec));
	aco_option_register(&cfg_info, "debug", ACO_EXACT,
		connection_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, debug));

	/* Integer fields */
	aco_option_register(&cfg_info, "message_max_bytes", ACO_EXACT,
		connection_options, default_message_max_bytes_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, message_max_bytes));
	aco_option_register(&cfg_info, "request_timeout_ms", ACO_EXACT,
		connection_options, default_request_timeout_ms_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, request_timeout_ms));
	aco_option_register(&cfg_info, "message_timeout_ms", ACO_EXACT,
		connection_options, default_message_timeout_ms_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, message_timeout_ms));
	aco_option_register(&cfg_info, "compression_level", ACO_EXACT,
		connection_options, default_compression_level_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, compression_level));
	aco_option_register(&cfg_info, "linger_ms", ACO_EXACT,
		connection_options, default_linger_ms_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, linger_ms));
	aco_option_register(&cfg_info, "batch_num_messages", ACO_EXACT,
		connection_options, default_batch_num_messages_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, batch_num_messages));
	aco_option_register(&cfg_info, "batch_size", ACO_EXACT,
		connection_options, default_batch_size_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, batch_size));
	aco_option_register(&cfg_info, "queue_buffering_max_messages", ACO_EXACT,
		connection_options, default_queue_buffering_max_messages_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, queue_buffering_max_messages));
	aco_option_register(&cfg_info, "queue_buffering_max_kbytes", ACO_EXACT,
		connection_options, default_queue_buffering_max_kbytes_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, queue_buffering_max_kbytes));
	aco_option_register(&cfg_info, "acks", ACO_EXACT,
		connection_options, default_acks_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, acks));
	aco_option_register(&cfg_info, "retries", ACO_EXACT,
		connection_options, default_retries_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, retries));
	aco_option_register(&cfg_info, "reconnect_backoff_ms", ACO_EXACT,
		connection_options, default_reconnect_backoff_ms_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, reconnect_backoff_ms));
	aco_option_register(&cfg_info, "reconnect_backoff_max_ms", ACO_EXACT,
		connection_options, default_reconnect_backoff_max_ms_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, reconnect_backoff_max_ms));

	/* Boolean fields */
	aco_option_register(&cfg_info, "enable_idempotence", ACO_EXACT,
		connection_options, "no", OPT_BOOL_T, 1,
		FLDSET(struct kafka_conf_connection, enable_idempotence));

	return process_config(0);
}

int kafka_config_reload(void)
{
	return process_config(1);
}

void kafka_config_destroy(void)
{
	aco_info_destroy(&cfg_info);
	ao2_global_obj_release(confs);
}

struct kafka_conf *kafka_config_get(void)
{
	struct kafka_conf *res = ao2_global_obj_ref(confs);
	if (!res) {
		ast_log(LOG_ERROR,
			"Error obtaining config from " CONF_FILENAME "\n");
	}
	return res;
}

struct kafka_conf_connection *kafka_config_get_connection(const char *name)
{
	RAII_VAR(struct kafka_conf *, conf, NULL, ao2_cleanup);
	conf = kafka_config_get();
	if (!conf) {
		return NULL;
	}

	return ao2_find(conf->connections, name, OBJ_SEARCH_KEY);
}
