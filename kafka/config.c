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

	if (ast_string_field_init(cxn_conf, 64) != 0) {
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

	snprintf(default_message_max_bytes_str, sizeof(default_message_max_bytes_str),
		"%d", 1000000);

	snprintf(default_request_timeout_ms_str, sizeof(default_request_timeout_ms_str),
		"%d", 5000);

	snprintf(default_message_timeout_ms_str, sizeof(default_message_timeout_ms_str),
		"%d", 300000);

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
	aco_option_register(&cfg_info, "brokers", ACO_EXACT,
		connection_options, "localhost:9092", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, brokers));
	aco_option_register(&cfg_info, "client_id", ACO_EXACT,
		connection_options, "asterisk", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct kafka_conf_connection, client_id));

	aco_option_register(&cfg_info, "message_max_bytes", ACO_EXACT,
		connection_options, default_message_max_bytes_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, message_max_bytes));

	aco_option_register(&cfg_info, "request_timeout_ms", ACO_EXACT,
		connection_options, default_request_timeout_ms_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, request_timeout_ms));

	aco_option_register(&cfg_info, "message_timeout_ms", ACO_EXACT,
		connection_options, default_message_timeout_ms_str, OPT_INT_T, 0,
		FLDSET(struct kafka_conf_connection, message_timeout_ms));

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
