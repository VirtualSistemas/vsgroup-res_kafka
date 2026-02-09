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
 * \brief Command line for Kafka.
 */

#include "asterisk.h"


#include "asterisk/cli.h"
#include "asterisk/kafka.h"
#include "internal.h"

#define CLI_NAME_WIDTH 15
#define CLI_BROKERS_WIDTH 30

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

	ast_cli(a->fd, "Name:              %s\n", cxn_conf->name);
	ast_cli(a->fd, "Brokers:           %s\n", cxn_conf->brokers);
	ast_cli(a->fd, "Client ID:         %s\n", cxn_conf->client_id);
	ast_cli(a->fd, "Message max bytes: %d\n", cxn_conf->message_max_bytes);
	ast_cli(a->fd, "Request timeout:   %d ms\n", cxn_conf->request_timeout_ms);
	ast_cli(a->fd, "Message timeout:   %d ms\n", cxn_conf->message_timeout_ms);

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

static struct ast_cli_entry kafka_cli[] = {
	AST_CLI_DEFINE(cli_show, "Show Kafka settings"),
	AST_CLI_DEFINE(cli_show_connection, "Show Kafka connection"),
	AST_CLI_DEFINE(cli_test_send, "Test sending a message"),
};

int kafka_cli_register(void)
{
	return ast_cli_register_multiple(kafka_cli, ARRAY_LEN(kafka_cli));
}

int kafka_cli_unregister(void)
{
	return ast_cli_unregister_multiple(kafka_cli, ARRAY_LEN(kafka_cli));
}
