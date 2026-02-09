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
	);

	/*! Maximum message size in bytes */
	int message_max_bytes;
	/*! Request timeout in milliseconds */
	int request_timeout_ms;
	/*! Message timeout in milliseconds (how long to buffer before giving up) */
	int message_timeout_ms;
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

#endif /* _ASTERISK_KAFKA_INTERNAL_H_ */
