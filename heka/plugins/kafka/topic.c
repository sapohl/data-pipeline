/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @brief Lua Kafka functions @file */

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

#include "lauxlib.h"
#include "lua.h"
#include "lua_kafka.h"

const char* luakafka_topic = "mozsvc.kafka.topic";
const char* luakafka_topic_table = "kafka.topic";


static luakafka_topic_t* check_topic(lua_State* lua, int min_args)
{
  luakafka_topic_t* ud = luaL_checkudata(lua, 1, luakafka_topic);
  luaL_argcheck(lua, min_args <= lua_gettop(lua), 0,
                "incorrect number of arguments");
  return ud;
}


static void load_conf(lua_State* lua, rd_kafka_topic_conf_t* conf)
{
  if (!conf) {
    luaL_error(lua, "rd_kafka_topic_conf_new() failed");
  }

  char errstr[512];
  lua_pushnil(lua);
  while (lua_next(lua, 3) != 0) {
    int kt = lua_type(lua, -2);
    int vt = lua_type(lua, -1);
    switch (kt) {
    case LUA_TSTRING:
      switch (vt) {
      case LUA_TSTRING:
      case LUA_TNUMBER:
      case LUA_TBOOLEAN:
        {
          const char* key = lua_tostring(lua, -2);
          const char* value = lua_tostring(lua, -1);
          if (value) {
            rd_kafka_conf_res_t r;
            r = rd_kafka_topic_conf_set(conf, key, value, errstr,
                                        sizeof errstr);
            if (r) luaL_error(lua, "Failed to set %s = %s : %s", key, value,
                              errstr);
          }
        }
        break;
      default:
        luaL_error(lua, "invalid config value type: %s", lua_typename(lua, vt));
      }
      break;
    default:
      luaL_error(lua, "invalid config key type: %s", lua_typename(lua, kt));
    }
    lua_pop(lua, 1);
  }
}


static int topic_new(lua_State* lua)
{

  luakafka_producer_t *kp;
  const char* topic;
  int n = lua_gettop(lua);
  if (n == 2 || n == 3) {
    kp = luaL_checkudata(lua, 1, luakafka_producer);
    topic = luaL_checkstring(lua, 2);
    if (!lua_isnone(lua, 3)) luaL_checktype(lua, 3, LUA_TTABLE);
  } else {
    luaL_argerror(lua, n, "incorrect number of arguments");
  }

  luakafka_topic_t* kt = lua_newuserdata(lua, sizeof(luakafka_topic_t));

  kt->conf = rd_kafka_topic_conf_new();
  if (n == 3) load_conf(lua, kt->conf);

  char errstr[512];
  kt->rkt = rd_kafka_topic_new(kp->rk, topic, kt->conf);
  if (!kt->rkt) {
    return luaL_error(lua, "Failed to create a Kafka topic: %s", errstr);
  }

  luaL_getmetatable(lua, luakafka_topic);
  lua_setmetatable(lua, -2);
  return 1;
}


static int topic_send(lua_State* lua)
{
  luakafka_topic_t *kt = check_topic(lua, 4);
  int32_t partition = (int32_t)luaL_checkinteger(lua, 2);
  size_t len;
  const char* msg = luaL_checklstring(lua, 3, &len);
  luaL_checktype(lua, 4, LUA_TLIGHTUSERDATA);
  void *sequence_id = lua_touserdata(lua, 4);

  int ret = rd_kafka_produce(kt->rkt, partition,
                             RD_KAFKA_MSG_F_COPY,
                             (void*) msg, len,
                             NULL, 0, // optional key/len
                             sequence_id // opaque pointer
                             );
  if (ret == -1) {
    lua_pushinteger(lua, errno);
  } else {
    lua_pushinteger(lua, 0);
  }
  return 1;
}


static int topic_gc(lua_State* lua)
{
  luakafka_topic_t *kt = check_topic(lua, 1);
  rd_kafka_topic_destroy(kt->rkt);
  return 0;
}


static const struct luaL_reg topiclib_f[] =
{
  { "new", topic_new },
  { NULL, NULL }
};


static const struct luaL_reg topiclib_m[] =
{
  { "send", topic_send },
  { "__gc", topic_gc },
  { NULL, NULL }
};


int luaopen_kafka_topic(lua_State* lua)
{
  luaL_newmetatable(lua, luakafka_topic);
  lua_pushvalue(lua, -1);
  lua_setfield(lua, -2, "__index");
  luaL_register(lua, NULL, topiclib_m);
  luaL_register(lua, luakafka_topic_table, topiclib_f);
  return 1;
}
