/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** Lua Kafka constants/types @file */

#ifndef lua_kafka_h_
#define lua_kafka_h_

extern const char* luakafka_producer;
extern const char* luakafka_producer_table;

extern const char* luakafka_topic;
extern const char* luakafka_topic_table;

typedef struct luakafka_producer_s {
  rd_kafka_conf_t* conf;
  rd_kafka_t* rk;
  void* msg_opaque;
  int failures;
} luakafka_producer_t;

typedef struct luakafka_topic_s {
  rd_kafka_topic_conf_t* conf;
  rd_kafka_topic_t* rkt;
} luakafka_topic_t;

#endif
