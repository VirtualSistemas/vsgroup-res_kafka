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
 * \brief Tests for res_kafka
 *
 * \author VSGroup
 */

/*** MODULEINFO
	<depend>TEST_FRAMEWORK</depend>
	<depend>res_kafka</depend>
	<support_level>core</support_level>
 ***/

#include "asterisk.h"

#include "asterisk/module.h"
#include "asterisk/test.h"
#include "asterisk/kafka.h"
#include "asterisk/utils.h"
#include "asterisk/lock.h"
#include "kafka/internal.h"

#define TEST_CATEGORY    "/res/kafka/"
#define TEST_CONNECTION  "test-kafka"
#define TEST_TOPIC_PREFIX "ast_test_kafka_"
#define WAIT_TIMEOUT_SEC 30

/* ---- Helpers ---- */

/*!
 * \brief Generate a unique test topic name.
 *
 * Writes a name like "ast_test_kafka_1a2b3c4d" into buf.
 *
 * \param buf   Destination buffer (must be at least 32 bytes).
 * \param size  Size of buffer.
 */
static void generate_test_topic(char *buf, size_t size)
{
	snprintf(buf, size, "%s%08lx", TEST_TOPIC_PREFIX, ast_random());
}

/*!
 * \brief Check if the test-kafka connection is configured.
 *
 * \param test  The test info (used for status output).
 * \return Non-zero if available, 0 if not.
 */
static int kafka_broker_available(struct ast_test *test)
{
	struct kafka_conf_connection *cxn;

	cxn = kafka_config_get_connection(TEST_CONNECTION);
	if (!cxn) {
		ast_test_status_update(test,
			"Skipping: connection '%s' not configured in kafka.conf\n",
			TEST_CONNECTION);
		return 0;
	}
	ao2_cleanup(cxn);
	return 1;
}

/*! \brief Data structure for consumer callback synchronization */
struct test_consumer_data {
	ast_mutex_t lock;
	ast_cond_t cond;
	int message_count;
	char payload[1024];
	size_t payload_len;
	char key[256];
	size_t key_len;
};

static void test_consumer_data_init(struct test_consumer_data *data)
{
	memset(data, 0, sizeof(*data));
	ast_mutex_init(&data->lock);
	ast_cond_init(&data->cond, NULL);
}

static void test_consumer_data_destroy(struct test_consumer_data *data)
{
	ast_mutex_destroy(&data->lock);
	ast_cond_destroy(&data->cond);
}

/*!
 * \brief Consumer callback that stores the received message and signals.
 */
static void test_message_cb(const char *topic, int32_t partition,
	int64_t offset, const void *payload, size_t len,
	const void *key, size_t key_len, void *userdata)
{
	struct test_consumer_data *data = userdata;

	ast_mutex_lock(&data->lock);

	if (len > sizeof(data->payload) - 1) {
		len = sizeof(data->payload) - 1;
	}
	memcpy(data->payload, payload, len);
	data->payload[len] = '\0';
	data->payload_len = len;

	if (key && key_len > 0) {
		size_t copy_len = key_len;
		if (copy_len > sizeof(data->key) - 1) {
			copy_len = sizeof(data->key) - 1;
		}
		memcpy(data->key, key, copy_len);
		data->key[copy_len] = '\0';
		data->key_len = copy_len;
	}

	data->message_count++;
	ast_cond_signal(&data->cond);
	ast_mutex_unlock(&data->lock);
}

/*!
 * \brief Wait for at least \a expected messages within \a timeout_sec seconds.
 *
 * \return 0 on success, -1 on timeout.
 */
static int wait_for_messages(struct test_consumer_data *data,
	int expected, int timeout_sec)
{
	struct timespec ts;
	int res = 0;

	clock_gettime(CLOCK_REALTIME, &ts);
	ts.tv_sec += timeout_sec;

	ast_mutex_lock(&data->lock);
	while (data->message_count < expected && res == 0) {
		res = ast_cond_timedwait(&data->cond, &data->lock, &ts);
	}
	res = (data->message_count >= expected) ? 0 : -1;
	ast_mutex_unlock(&data->lock);

	return res;
}

/* ---- Config tests ---- */

AST_TEST_DEFINE(config_load)
{
	struct kafka_conf *conf;

	switch (cmd) {
	case TEST_INIT:
		info->name = "config_load";
		info->category = TEST_CATEGORY;
		info->summary = "Verify kafka config loads successfully";
		info->description =
			"Checks that kafka_config_get() returns a valid config "
			"object with general and connections sections populated.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	conf = kafka_config_get();
	if (!conf) {
		ast_test_status_update(test, "kafka_config_get() returned NULL\n");
		return AST_TEST_FAIL;
	}

	if (!conf->general) {
		ast_test_status_update(test, "config->general is NULL\n");
		ao2_cleanup(conf);
		return AST_TEST_FAIL;
	}

	if (!conf->connections) {
		ast_test_status_update(test, "config->connections is NULL\n");
		ao2_cleanup(conf);
		return AST_TEST_FAIL;
	}

	ao2_cleanup(conf);
	return AST_TEST_PASS;
}

AST_TEST_DEFINE(config_connection_lookup)
{
	struct kafka_conf_connection *cxn;

	switch (cmd) {
	case TEST_INIT:
		info->name = "config_connection_lookup";
		info->category = TEST_CATEGORY;
		info->summary = "Verify connection lookup by name";
		info->description =
			"Checks that kafka_config_get_connection() finds a known "
			"connection and returns NULL for an unknown one.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	cxn = kafka_config_get_connection(TEST_CONNECTION);
	if (!cxn) {
		ast_test_status_update(test,
			"kafka_config_get_connection(\"%s\") returned NULL\n",
			TEST_CONNECTION);
		return AST_TEST_FAIL;
	}
	ao2_cleanup(cxn);

	/* Non-existent connection must return NULL */
	cxn = kafka_config_get_connection("xyz_nonexistent_connection");
	if (cxn) {
		ast_test_status_update(test,
			"kafka_config_get_connection(\"xyz_nonexistent_connection\") "
			"should have returned NULL\n");
		ao2_cleanup(cxn);
		return AST_TEST_FAIL;
	}

	return AST_TEST_PASS;
}

/* ---- Producer tests ---- */

AST_TEST_DEFINE(producer_get)
{
	struct ast_kafka_producer *p1;
	struct ast_kafka_producer *p2;

	switch (cmd) {
	case TEST_INIT:
		info->name = "producer_get";
		info->category = TEST_CATEGORY;
		info->summary = "Get a producer and verify caching";
		info->description =
			"Checks that ast_kafka_get_producer() returns a non-NULL "
			"producer and that a second call returns the same cached instance.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	p1 = ast_kafka_get_producer(TEST_CONNECTION);
	if (!p1) {
		ast_test_status_update(test,
			"ast_kafka_get_producer(\"%s\") returned NULL\n",
			TEST_CONNECTION);
		return AST_TEST_FAIL;
	}

	p2 = ast_kafka_get_producer(TEST_CONNECTION);
	if (!p2) {
		ast_test_status_update(test,
			"Second ast_kafka_get_producer() returned NULL\n");
		ao2_cleanup(p1);
		return AST_TEST_FAIL;
	}

	if (p1 != p2) {
		ast_test_status_update(test,
			"Second call returned different pointer (cache broken)\n");
		ao2_cleanup(p1);
		ao2_cleanup(p2);
		return AST_TEST_FAIL;
	}

	ao2_cleanup(p1);
	ao2_cleanup(p2);
	return AST_TEST_PASS;
}

AST_TEST_DEFINE(producer_get_nonexistent)
{
	struct ast_kafka_producer *p;

	switch (cmd) {
	case TEST_INIT:
		info->name = "producer_get_nonexistent";
		info->category = TEST_CATEGORY;
		info->summary = "Get a producer for a non-existent connection";
		info->description =
			"Checks that ast_kafka_get_producer() returns NULL "
			"for an unknown connection name.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	p = ast_kafka_get_producer("xyz_nonexistent_connection");
	if (p) {
		ast_test_status_update(test,
			"ast_kafka_get_producer(\"xyz_nonexistent_connection\") "
			"should have returned NULL\n");
		ao2_cleanup(p);
		return AST_TEST_FAIL;
	}

	return AST_TEST_PASS;
}

AST_TEST_DEFINE(produce_message)
{
	struct ast_kafka_producer *producer;
	char topic[64];
	int res;

	switch (cmd) {
	case TEST_INIT:
		info->name = "produce_message";
		info->category = TEST_CATEGORY;
		info->summary = "Produce messages with various parameters";
		info->description =
			"Checks that ast_kafka_produce() succeeds with key, "
			"without key, and with empty payload.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	producer = ast_kafka_get_producer(TEST_CONNECTION);
	if (!producer) {
		ast_test_status_update(test, "Failed to get producer\n");
		return AST_TEST_FAIL;
	}

	generate_test_topic(topic, sizeof(topic));

	/* With key */
	res = ast_kafka_produce(producer, topic, "test-key", "hello", 5);
	if (res != 0) {
		ast_test_status_update(test, "produce with key failed\n");
		ao2_cleanup(producer);
		return AST_TEST_FAIL;
	}

	/* Without key */
	res = ast_kafka_produce(producer, topic, NULL, "world", 5);
	if (res != 0) {
		ast_test_status_update(test, "produce without key failed\n");
		ao2_cleanup(producer);
		return AST_TEST_FAIL;
	}

	/* Empty payload */
	res = ast_kafka_produce(producer, topic, NULL, "", 0);
	if (res != 0) {
		ast_test_status_update(test, "produce with empty payload failed\n");
		ao2_cleanup(producer);
		return AST_TEST_FAIL;
	}

	ao2_cleanup(producer);
	return AST_TEST_PASS;
}

AST_TEST_DEFINE(produce_null_params)
{
	int res;

	switch (cmd) {
	case TEST_INIT:
		info->name = "produce_null_params";
		info->category = TEST_CATEGORY;
		info->summary = "Produce with NULL producer returns error";
		info->description =
			"Checks that ast_kafka_produce(NULL, ...) returns -1.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	res = ast_kafka_produce(NULL, "topic", NULL, "data", 4);
	if (res != -1) {
		ast_test_status_update(test,
			"ast_kafka_produce(NULL, ...) should return -1, got %d\n", res);
		return AST_TEST_FAIL;
	}

	return AST_TEST_PASS;
}

/* ---- Consumer tests ---- */

AST_TEST_DEFINE(consumer_get)
{
	struct ast_kafka_consumer *c1;
	struct ast_kafka_consumer *c2;

	switch (cmd) {
	case TEST_INIT:
		info->name = "consumer_get";
		info->category = TEST_CATEGORY;
		info->summary = "Get a consumer and verify caching";
		info->description =
			"Checks that ast_kafka_get_consumer() returns a non-NULL "
			"consumer and that a second call returns the same cached instance.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	c1 = ast_kafka_get_consumer(TEST_CONNECTION);
	if (!c1) {
		ast_test_status_update(test,
			"ast_kafka_get_consumer(\"%s\") returned NULL\n",
			TEST_CONNECTION);
		return AST_TEST_FAIL;
	}

	c2 = ast_kafka_get_consumer(TEST_CONNECTION);
	if (!c2) {
		ast_test_status_update(test,
			"Second ast_kafka_get_consumer() returned NULL\n");
		ao2_cleanup(c1);
		return AST_TEST_FAIL;
	}

	if (c1 != c2) {
		ast_test_status_update(test,
			"Second call returned different pointer (cache broken)\n");
		ao2_cleanup(c1);
		ao2_cleanup(c2);
		return AST_TEST_FAIL;
	}

	ao2_cleanup(c1);
	ao2_cleanup(c2);
	return AST_TEST_PASS;
}

AST_TEST_DEFINE(consumer_get_nonexistent)
{
	struct ast_kafka_consumer *c;

	switch (cmd) {
	case TEST_INIT:
		info->name = "consumer_get_nonexistent";
		info->category = TEST_CATEGORY;
		info->summary = "Get a consumer for a non-existent connection";
		info->description =
			"Checks that ast_kafka_get_consumer() returns NULL "
			"for an unknown connection name.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	c = ast_kafka_get_consumer("xyz_nonexistent_connection");
	if (c) {
		ast_test_status_update(test,
			"ast_kafka_get_consumer(\"xyz_nonexistent_connection\") "
			"should have returned NULL\n");
		ao2_cleanup(c);
		return AST_TEST_FAIL;
	}

	return AST_TEST_PASS;
}

AST_TEST_DEFINE(consumer_subscribe_unsubscribe)
{
	struct ast_kafka_consumer *consumer;
	char topic[64];
	int res;

	switch (cmd) {
	case TEST_INIT:
		info->name = "consumer_subscribe_unsubscribe";
		info->category = TEST_CATEGORY;
		info->summary = "Subscribe and unsubscribe a consumer";
		info->description =
			"Checks that subscribe returns 0, "
			"kafka_consumer_is_subscribed() confirms subscription, "
			"and unsubscribe returns 0.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	consumer = ast_kafka_get_consumer(TEST_CONNECTION);
	if (!consumer) {
		ast_test_status_update(test, "Failed to get consumer\n");
		return AST_TEST_FAIL;
	}

	generate_test_topic(topic, sizeof(topic));

	res = ast_kafka_consumer_subscribe(consumer, topic,
		test_message_cb, NULL);
	if (res != 0) {
		ast_test_status_update(test, "subscribe failed\n");
		ao2_cleanup(consumer);
		return AST_TEST_FAIL;
	}

	if (!kafka_consumer_is_subscribed(consumer)) {
		ast_test_status_update(test,
			"kafka_consumer_is_subscribed() returned 0 after subscribe\n");
		ast_kafka_consumer_unsubscribe(consumer);
		ao2_cleanup(consumer);
		return AST_TEST_FAIL;
	}

	res = ast_kafka_consumer_unsubscribe(consumer);
	if (res != 0) {
		ast_test_status_update(test, "unsubscribe failed\n");
		ao2_cleanup(consumer);
		return AST_TEST_FAIL;
	}

	if (kafka_consumer_is_subscribed(consumer)) {
		ast_test_status_update(test,
			"kafka_consumer_is_subscribed() returned non-zero "
			"after unsubscribe\n");
		ao2_cleanup(consumer);
		return AST_TEST_FAIL;
	}

	ao2_cleanup(consumer);
	return AST_TEST_PASS;
}

AST_TEST_DEFINE(consumer_subscribe_null_params)
{
	int res;

	switch (cmd) {
	case TEST_INIT:
		info->name = "consumer_subscribe_null_params";
		info->category = TEST_CATEGORY;
		info->summary = "Subscribe/unsubscribe with NULL params";
		info->description =
			"Checks that subscribe and unsubscribe with NULL consumer "
			"return -1.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	res = ast_kafka_consumer_subscribe(NULL, "topic",
		test_message_cb, NULL);
	if (res != -1) {
		ast_test_status_update(test,
			"subscribe(NULL, ...) should return -1, got %d\n", res);
		return AST_TEST_FAIL;
	}

	res = ast_kafka_consumer_unsubscribe(NULL);
	if (res != -1) {
		ast_test_status_update(test,
			"unsubscribe(NULL) should return -1, got %d\n", res);
		return AST_TEST_FAIL;
	}

	return AST_TEST_PASS;
}

/* ---- Admin tests ---- */

AST_TEST_DEFINE(ensure_topic)
{
	struct ast_kafka_producer *producer;
	char topic[64];
	int res;

	switch (cmd) {
	case TEST_INIT:
		info->name = "ensure_topic";
		info->category = TEST_CATEGORY;
		info->summary = "Ensure topic creation and idempotency";
		info->description =
			"Creates a topic and verifies that a second call with the "
			"same name also returns 0 (topic already exists).";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	producer = ast_kafka_get_producer(TEST_CONNECTION);
	if (!producer) {
		ast_test_status_update(test, "Failed to get producer\n");
		return AST_TEST_FAIL;
	}

	generate_test_topic(topic, sizeof(topic));

	/* First call: create */
	res = ast_kafka_ensure_topic(producer, topic, 1, 1);
	if (res != 0) {
		ast_test_status_update(test,
			"ensure_topic first call failed for '%s'\n", topic);
		ao2_cleanup(producer);
		return AST_TEST_FAIL;
	}

	/* Second call: already exists */
	res = ast_kafka_ensure_topic(producer, topic, 1, 1);
	if (res != 0) {
		ast_test_status_update(test,
			"ensure_topic second call failed for '%s'\n", topic);
		ao2_cleanup(producer);
		return AST_TEST_FAIL;
	}

	ao2_cleanup(producer);
	return AST_TEST_PASS;
}

AST_TEST_DEFINE(ensure_topic_null_params)
{
	struct ast_kafka_producer *producer;
	int res;

	switch (cmd) {
	case TEST_INIT:
		info->name = "ensure_topic_null_params";
		info->category = TEST_CATEGORY;
		info->summary = "Ensure topic with NULL params returns error";
		info->description =
			"Checks that ast_kafka_ensure_topic() with NULL producer "
			"or NULL topic returns -1.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	/* NULL producer */
	res = ast_kafka_ensure_topic(NULL, "sometopic", 1, 1);
	if (res != -1) {
		ast_test_status_update(test,
			"ensure_topic(NULL, ...) should return -1, got %d\n", res);
		return AST_TEST_FAIL;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	/* NULL topic */
	producer = ast_kafka_get_producer(TEST_CONNECTION);
	if (!producer) {
		ast_test_status_update(test, "Failed to get producer\n");
		return AST_TEST_FAIL;
	}

	res = ast_kafka_ensure_topic(producer, NULL, 1, 1);
	if (res != -1) {
		ast_test_status_update(test,
			"ensure_topic(producer, NULL, ...) should return -1, got %d\n",
			res);
		ao2_cleanup(producer);
		return AST_TEST_FAIL;
	}

	ao2_cleanup(producer);
	return AST_TEST_PASS;
}

/* ---- End-to-end test ---- */

AST_TEST_DEFINE(produce_consume_roundtrip)
{
	struct ast_kafka_producer *producer = NULL;
	struct ast_kafka_consumer *consumer = NULL;
	struct test_consumer_data data;
	char topic[64];
	const char *test_payload = "roundtrip_test_payload";
	const char *test_key = "roundtrip_key";
	enum ast_test_result_state result = AST_TEST_FAIL;

	switch (cmd) {
	case TEST_INIT:
		info->name = "produce_consume_roundtrip";
		info->category = TEST_CATEGORY;
		info->summary = "End-to-end produce and consume roundtrip";
		info->description =
			"Creates a topic, subscribes a consumer, produces a message, "
			"and verifies the consumer callback receives the correct "
			"payload and key. This test requires a running Kafka broker.";
		info->explicit_only = 1;
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	test_consumer_data_init(&data);

	producer = ast_kafka_get_producer(TEST_CONNECTION);
	if (!producer) {
		ast_test_status_update(test, "Failed to get producer\n");
		goto cleanup;
	}

	consumer = ast_kafka_get_consumer(TEST_CONNECTION);
	if (!consumer) {
		ast_test_status_update(test, "Failed to get consumer\n");
		goto cleanup;
	}

	generate_test_topic(topic, sizeof(topic));

	/* Create topic first */
	if (ast_kafka_ensure_topic(producer, topic, 1, 1) != 0) {
		ast_test_status_update(test,
			"Failed to create topic '%s'\n", topic);
		goto cleanup;
	}

	/* Subscribe consumer */
	if (ast_kafka_consumer_subscribe(consumer, topic,
		test_message_cb, &data) != 0) {
		ast_test_status_update(test, "Failed to subscribe consumer\n");
		goto cleanup;
	}

	/* Give the consumer time to join the group and get partition assignment */
	usleep(2000000); /* 2 seconds */

	/* Produce message */
	if (ast_kafka_produce(producer, topic, test_key,
		test_payload, strlen(test_payload)) != 0) {
		ast_test_status_update(test, "Failed to produce message\n");
		goto cleanup;
	}

	/* Wait for the message to arrive */
	if (wait_for_messages(&data, 1, WAIT_TIMEOUT_SEC) != 0) {
		ast_test_status_update(test,
			"Timeout waiting for message (waited %d seconds)\n",
			WAIT_TIMEOUT_SEC);
		goto cleanup;
	}

	/* Validate payload */
	if (strcmp(data.payload, test_payload) != 0) {
		ast_test_status_update(test,
			"Payload mismatch: expected '%s', got '%s'\n",
			test_payload, data.payload);
		goto cleanup;
	}

	/* Validate key */
	if (strcmp(data.key, test_key) != 0) {
		ast_test_status_update(test,
			"Key mismatch: expected '%s', got '%s'\n",
			test_key, data.key);
		goto cleanup;
	}

	ast_test_status_update(test, "Roundtrip successful: payload and key match\n");
	result = AST_TEST_PASS;

cleanup:
	if (consumer) {
		ast_kafka_consumer_unsubscribe(consumer);
		ao2_cleanup(consumer);
	}
	ao2_cleanup(producer);
	test_consumer_data_destroy(&data);
	return result;
}

/* ---- Internal API tests ---- */

struct foreach_test_data {
	int count;
	int found_valid_name;
};

static int foreach_test_cb(struct ast_kafka_consumer *consumer, void *arg)
{
	struct foreach_test_data *data = arg;
	const char *name;

	data->count++;
	name = kafka_consumer_get_name(consumer);
	if (name && strlen(name) > 0) {
		data->found_valid_name = 1;
	}

	return 0; /* continue iterating */
}

AST_TEST_DEFINE(consumer_foreach)
{
	struct ast_kafka_consumer *consumer;
	struct foreach_test_data data = { 0, 0 };

	switch (cmd) {
	case TEST_INIT:
		info->name = "consumer_foreach";
		info->category = TEST_CATEGORY;
		info->summary = "Iterate active consumers";
		info->description =
			"Creates a consumer, then uses kafka_foreach_consumer() "
			"to iterate and verify kafka_consumer_get_name() returns "
			"a valid string.";
		return AST_TEST_NOT_RUN;
	case TEST_EXECUTE:
		break;
	}

	if (!kafka_broker_available(test)) {
		return AST_TEST_NOT_RUN;
	}

	/* Ensure at least one consumer exists */
	consumer = ast_kafka_get_consumer(TEST_CONNECTION);
	if (!consumer) {
		ast_test_status_update(test, "Failed to get consumer\n");
		return AST_TEST_FAIL;
	}

	kafka_foreach_consumer(foreach_test_cb, &data);

	ao2_cleanup(consumer);

	if (data.count == 0) {
		ast_test_status_update(test,
			"kafka_foreach_consumer() iterated 0 consumers\n");
		return AST_TEST_FAIL;
	}

	if (!data.found_valid_name) {
		ast_test_status_update(test,
			"kafka_consumer_get_name() did not return a valid name\n");
		return AST_TEST_FAIL;
	}

	return AST_TEST_PASS;
}

/* ---- Module lifecycle ---- */

static int load_module(void)
{
	AST_TEST_REGISTER(config_load);
	AST_TEST_REGISTER(config_connection_lookup);
	AST_TEST_REGISTER(producer_get);
	AST_TEST_REGISTER(producer_get_nonexistent);
	AST_TEST_REGISTER(produce_message);
	AST_TEST_REGISTER(produce_null_params);
	AST_TEST_REGISTER(consumer_get);
	AST_TEST_REGISTER(consumer_get_nonexistent);
	AST_TEST_REGISTER(consumer_subscribe_unsubscribe);
	AST_TEST_REGISTER(consumer_subscribe_null_params);
	AST_TEST_REGISTER(ensure_topic);
	AST_TEST_REGISTER(ensure_topic_null_params);
	AST_TEST_REGISTER(produce_consume_roundtrip);
	AST_TEST_REGISTER(consumer_foreach);

	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void)
{
	AST_TEST_UNREGISTER(config_load);
	AST_TEST_UNREGISTER(config_connection_lookup);
	AST_TEST_UNREGISTER(producer_get);
	AST_TEST_UNREGISTER(producer_get_nonexistent);
	AST_TEST_UNREGISTER(produce_message);
	AST_TEST_UNREGISTER(produce_null_params);
	AST_TEST_UNREGISTER(consumer_get);
	AST_TEST_UNREGISTER(consumer_get_nonexistent);
	AST_TEST_UNREGISTER(consumer_subscribe_unsubscribe);
	AST_TEST_UNREGISTER(consumer_subscribe_null_params);
	AST_TEST_UNREGISTER(ensure_topic);
	AST_TEST_UNREGISTER(ensure_topic_null_params);
	AST_TEST_UNREGISTER(produce_consume_roundtrip);
	AST_TEST_UNREGISTER(consumer_foreach);

	return 0;
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_DEFAULT, "Kafka Resource Module Tests",
	.support_level = AST_MODULE_SUPPORT_CORE,
	.load = load_module,
	.unload = unload_module,
	.requires = "res_kafka",
);
