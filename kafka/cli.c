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
 * \brief Command line for Kafka.
 */

#include "asterisk.h"


#include "asterisk/cli.h"
#include "asterisk/kafka.h"
#include "internal.h"

#define CLI_NAME_WIDTH 15
#define CLI_BROKERS_WIDTH 30
#define CLI_SUBSCRIBED_WIDTH 12

static int cli_show_connection_summary(void *obj, void *arg, int flags)
{
	struct ast_cli_args *a = arg;
	struct kafka_conf_connection *cxn = obj;

	ast_cli(a->fd, "%-*s %-*s\n", CLI_NAME_WIDTH, cxn->name, CLI_BROKERS_WIDTH, cxn->brokers);

	return 0;
}

static char *cli_show(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a)
{
	RAII_VAR(struct kafka_conf *, conf, NULL, ao2_cleanup);

	switch (cmd) {
	case CLI_INIT:
		e->command = "kafka show status";
		e->usage =
		"usage: kafka show status\n"
		 "	 Shows all Kafka settings and status\n";
		return NULL;
	case CLI_GENERATE:
		return NULL;
	default:
		break;
	}

	if (a->argc != 3) {
		return CLI_SHOWUSAGE;
	}

	conf = kafka_config_get();
	if (!conf) {
		ast_cli(a->fd, "Error getting Kafka configuration\n");
		return CLI_FAILURE;
	}

	if (!conf->general->enabled) {
		ast_cli(a->fd, "Kafka disabled\n");
		return NULL;
	}

	ast_cli(a->fd, "Connections:\n");
	ast_cli(a->fd, "%-*s %-*s\n", CLI_NAME_WIDTH, "Name", CLI_BROKERS_WIDTH, "Brokers");
	ao2_callback(conf->connections, OBJ_NODATA,
		cli_show_connection_summary, a);

	return NULL;
}

static char *cli_complete_connection(
	const char *line, const char *word, int state)
{
	RAII_VAR(struct kafka_conf *, conf, kafka_config_get(), ao2_cleanup);
	struct kafka_conf_connection *cxn_conf;
	int which = 0;
	int wordlen = strlen(word);
	char *c = NULL;
	struct ao2_iterator i;

	if (!conf) {
		ast_log(LOG_ERROR, "Error getting Kafka configuration\n");
		return NULL;
	}

	i = ao2_iterator_init(conf->connections, 0);
	while ((cxn_conf = ao2_iterator_next(&i))) {
		if (!strncasecmp(word, cxn_conf->name, wordlen) && ++which > state) {
			c = ast_strdup(cxn_conf->name);
		}

		ao2_cleanup(cxn_conf);
		if (c) {
			break;
		}
	}
	ao2_iterator_destroy(&i);

	return c;
}

static char *cli_show_connection(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a)
{
	RAII_VAR(struct kafka_conf *, conf, NULL, ao2_cleanup);
	RAII_VAR(struct kafka_conf_connection *, cxn_conf, NULL, ao2_cleanup);

	switch (cmd) {
	case CLI_INIT:
		e->command = "kafka show connection";
		e->usage =
			"usage: kafka show connection <name>\n"
			 "	 Shows Kafka connection\n";
		return NULL;
	case CLI_GENERATE:
		if (a->pos > 3) {
			return NULL;
		}
		return cli_complete_connection(a->line, a->word, a->n);
	default:
		break;
	}

	if (a->argc != 4) {
		return CLI_SHOWUSAGE;
	}

	conf = kafka_config_get();
	if (!conf) {
		ast_cli(a->fd, "Error getting Kafka configuration\n");
		return CLI_FAILURE;
	}

	cxn_conf = ao2_find(conf->connections, a->argv[3], OBJ_SEARCH_KEY);
	if (!cxn_conf) {
		ast_cli(a->fd, "No connection named %s\n", a->argv[3]);
		return NULL;
	}

	ast_cli(a->fd, "Name:                          %s\n", cxn_conf->name);
	ast_cli(a->fd, "Brokers:                       %s\n", cxn_conf->brokers);
	ast_cli(a->fd, "Client ID:                     %s\n", cxn_conf->client_id);
	ast_cli(a->fd, "Message max bytes:             %d\n", cxn_conf->message_max_bytes);
	ast_cli(a->fd, "Request timeout:               %d ms\n", cxn_conf->request_timeout_ms);
	ast_cli(a->fd, "Message timeout:               %d ms\n", cxn_conf->message_timeout_ms);
	ast_cli(a->fd, "Security protocol:             %s\n", cxn_conf->security_protocol);
	if (!ast_strlen_zero(cxn_conf->sasl_mechanisms)) {
		ast_cli(a->fd, "SASL mechanisms:               %s\n", cxn_conf->sasl_mechanisms);
		ast_cli(a->fd, "SASL username:                 %s\n", cxn_conf->sasl_username);
		ast_cli(a->fd, "SASL password:                 %s\n",
			ast_strlen_zero(cxn_conf->sasl_password) ? "" : "***");
	}
	if (!ast_strlen_zero(cxn_conf->ssl_ca_location)) {
		ast_cli(a->fd, "SSL CA location:               %s\n", cxn_conf->ssl_ca_location);
	}
	if (!ast_strlen_zero(cxn_conf->ssl_certificate_location)) {
		ast_cli(a->fd, "SSL certificate location:      %s\n", cxn_conf->ssl_certificate_location);
	}
	if (!ast_strlen_zero(cxn_conf->ssl_key_location)) {
		ast_cli(a->fd, "SSL key location:              %s\n", cxn_conf->ssl_key_location);
	}
	ast_cli(a->fd, "Compression codec:             %s\n", cxn_conf->compression_codec);
	ast_cli(a->fd, "Compression level:             %d\n", cxn_conf->compression_level);
	ast_cli(a->fd, "Linger ms:                     %d\n", cxn_conf->linger_ms);
	ast_cli(a->fd, "Batch num messages:            %d\n", cxn_conf->batch_num_messages);
	ast_cli(a->fd, "Batch size:                    %d\n", cxn_conf->batch_size);
	ast_cli(a->fd, "Queue buffering max messages:  %d\n", cxn_conf->queue_buffering_max_messages);
	ast_cli(a->fd, "Queue buffering max kbytes:    %d\n", cxn_conf->queue_buffering_max_kbytes);
	ast_cli(a->fd, "Acks:                          %d\n", cxn_conf->acks);
	ast_cli(a->fd, "Retries:                       %d\n", cxn_conf->retries);
	ast_cli(a->fd, "Enable idempotence:            %s\n", cxn_conf->enable_idempotence ? "yes" : "no");
	ast_cli(a->fd, "Reconnect backoff:             %d ms\n", cxn_conf->reconnect_backoff_ms);
	ast_cli(a->fd, "Reconnect backoff max:         %d ms\n", cxn_conf->reconnect_backoff_max_ms);
	if (!ast_strlen_zero(cxn_conf->debug)) {
		ast_cli(a->fd, "Debug:                         %s\n", cxn_conf->debug);
	}
	/* Consumer-specific config */
	if (!ast_strlen_zero(cxn_conf->group_id)) {
		ast_cli(a->fd, "Group ID:                      %s\n", cxn_conf->group_id);
		ast_cli(a->fd, "Auto offset reset:             %s\n", cxn_conf->auto_offset_reset);
		ast_cli(a->fd, "Enable auto commit:            %s\n", cxn_conf->enable_auto_commit ? "yes" : "no");
		ast_cli(a->fd, "Auto commit interval:          %d ms\n", cxn_conf->auto_commit_interval_ms);
		ast_cli(a->fd, "Session timeout:               %d ms\n", cxn_conf->session_timeout_ms);
		ast_cli(a->fd, "Max poll interval:             %d ms\n", cxn_conf->max_poll_interval_ms);
	}

	return NULL;
}

static char *cli_test_send(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a)
{
	RAII_VAR(struct ast_kafka_producer *, producer, NULL, ao2_cleanup);
	const char *key = NULL;

	switch (cmd) {
	case CLI_INIT:
		e->command = "kafka test send connection";
		e->usage =
			"usage: kafka test send connection <name> topic <topic> message <message> [key <key>]\n"
			"       Sends a message to the specified Kafka topic\n";
		return NULL;
	case CLI_GENERATE:
		switch (a->pos) {
		case 4:
			return cli_complete_connection(a->line, a->word, a->n);
		case 5:
			return a->n == 0 ? ast_strdup("topic") : NULL;
		case 7:
			return a->n == 0 ? ast_strdup("message") : NULL;
		case 9:
			return a->n == 0 ? ast_strdup("key") : NULL;
		default:
			return NULL;
		}
	default:
		break;
	}

	if (a->argc != 9 && a->argc != 11) {
		return CLI_SHOWUSAGE;
	}

	if (strcasecmp(a->argv[5], "topic") != 0) {
		return CLI_SHOWUSAGE;
	}

	if (strcasecmp(a->argv[7], "message") != 0) {
		return CLI_SHOWUSAGE;
	}

	if (a->argc == 11) {
		if (strcasecmp(a->argv[9], "key") != 0) {
			return CLI_SHOWUSAGE;
		}
		key = a->argv[10];
	}

	producer = ast_kafka_get_producer(a->argv[4]);
	if (!producer) {
		ast_cli(a->fd, "No connection named %s\n", a->argv[4]);
		return NULL;
	}

	if (ast_kafka_produce(producer,
		a->argv[6],  /* topic */
		key,          /* key */
		a->argv[8],  /* payload */
		strlen(a->argv[8])) != 0) {
		ast_cli(a->fd, "Error sending message\n");
		return NULL;
	}

	ast_cli(a->fd, "Message sent successfully\n");
	return NULL;
}

/* ---- Consumer CLI ---- */

struct cli_consumers_arg {
	struct ast_cli_args *a;
};

static int cli_show_consumer_summary_cb(struct ast_kafka_consumer *consumer, void *arg)
{
	struct cli_consumers_arg *carg = arg;
	struct ast_cli_args *a = carg->a;

	ast_cli(a->fd, "%-*s %-*s\n",
		CLI_NAME_WIDTH, kafka_consumer_get_name(consumer),
		CLI_SUBSCRIBED_WIDTH, kafka_consumer_is_subscribed(consumer) ? "yes" : "no");

	return 0;
}

static char *cli_show_consumers(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a)
{
	struct cli_consumers_arg carg;

	switch (cmd) {
	case CLI_INIT:
		e->command = "kafka show consumers";
		e->usage =
			"usage: kafka show consumers\n"
			"       Shows all active Kafka consumers\n";
		return NULL;
	case CLI_GENERATE:
		return NULL;
	default:
		break;
	}

	if (a->argc != 3) {
		return CLI_SHOWUSAGE;
	}

	ast_cli(a->fd, "Active consumers:\n");
	ast_cli(a->fd, "%-*s %-*s\n",
		CLI_NAME_WIDTH, "Name",
		CLI_SUBSCRIBED_WIDTH, "Subscribed");

	carg.a = a;
	kafka_foreach_consumer(cli_show_consumer_summary_cb, &carg);

	return NULL;
}

struct cli_consumer_find_arg {
	const char *name;
	struct ast_kafka_consumer *found;
};

static int cli_consumer_find_cb(struct ast_kafka_consumer *consumer, void *arg)
{
	struct cli_consumer_find_arg *farg = arg;

	if (!strcmp(kafka_consumer_get_name(consumer), farg->name)) {
		farg->found = consumer;
		ao2_ref(consumer, +1);
		return 1; /* stop iteration */
	}

	return 0;
}

static char *cli_show_consumer(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a)
{
	RAII_VAR(struct kafka_conf_connection *, cxn_conf, NULL, ao2_cleanup);
	struct cli_consumer_find_arg farg;

	switch (cmd) {
	case CLI_INIT:
		e->command = "kafka show consumer";
		e->usage =
			"usage: kafka show consumer <name>\n"
			"       Shows details of a specific Kafka consumer\n";
		return NULL;
	case CLI_GENERATE:
		if (a->pos > 3) {
			return NULL;
		}
		return cli_complete_connection(a->line, a->word, a->n);
	default:
		break;
	}

	if (a->argc != 4) {
		return CLI_SHOWUSAGE;
	}

	farg.name = a->argv[3];
	farg.found = NULL;
	kafka_foreach_consumer(cli_consumer_find_cb, &farg);

	if (!farg.found) {
		ast_cli(a->fd, "No active consumer named %s\n", a->argv[3]);
		return NULL;
	}

	ast_cli(a->fd, "Name:                          %s\n", kafka_consumer_get_name(farg.found));
	ast_cli(a->fd, "Subscribed:                    %s\n", kafka_consumer_is_subscribed(farg.found) ? "yes" : "no");

	ao2_ref(farg.found, -1);

	/* Show config details */
	cxn_conf = kafka_config_get_connection(a->argv[3]);
	if (cxn_conf) {
		ast_cli(a->fd, "Brokers:                       %s\n", cxn_conf->brokers);
		ast_cli(a->fd, "Group ID:                      %s\n", cxn_conf->group_id);
		ast_cli(a->fd, "Auto offset reset:             %s\n", cxn_conf->auto_offset_reset);
		ast_cli(a->fd, "Enable auto commit:            %s\n", cxn_conf->enable_auto_commit ? "yes" : "no");
		ast_cli(a->fd, "Auto commit interval:          %d ms\n", cxn_conf->auto_commit_interval_ms);
		ast_cli(a->fd, "Session timeout:               %d ms\n", cxn_conf->session_timeout_ms);
		ast_cli(a->fd, "Max poll interval:             %d ms\n", cxn_conf->max_poll_interval_ms);
	}

	return NULL;
}

static struct ast_cli_entry kafka_cli[] = {
	AST_CLI_DEFINE(cli_show, "Show Kafka settings"),
	AST_CLI_DEFINE(cli_show_connection, "Show Kafka connection"),
	AST_CLI_DEFINE(cli_test_send, "Test sending a message"),
	AST_CLI_DEFINE(cli_show_consumers, "Show active Kafka consumers"),
	AST_CLI_DEFINE(cli_show_consumer, "Show Kafka consumer details"),
};

int kafka_cli_register(void)
{
	return ast_cli_register_multiple(kafka_cli, ARRAY_LEN(kafka_cli));
}

int kafka_cli_unregister(void)
{
	return ast_cli_unregister_multiple(kafka_cli, ARRAY_LEN(kafka_cli));
}
