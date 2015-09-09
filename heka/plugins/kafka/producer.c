/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @brief Lua Kafka functions @file */

#include <errno.h>
#include <float.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

#include "lauxlib.h"
#include "lua.h"
#include "lua_kafka.h"

const char* luakafka_producer = "mozsvc.kafka.producer";
const char* lualuakafka_producer_table = "kafka.producer";

static const char* async_checkpoint = "async_checkpoint_update";

static luakafka_producer_t* check_producer(lua_State* lua, int min_args)
{
  luakafka_producer_t* ud = luaL_checkudata(lua, 1, luakafka_producer);
  luaL_argcheck(lua, ud != NULL, 1, "invalid userdata type");
  luaL_argcheck(lua, min_args <= lua_gettop(lua), 0,
                "incorrect number of arguments");
  return ud;
}


static void msg_delivered(rd_kafka_t* rk,
                          void* payload,
                          size_t len,
                          int error_code,
                          void* opaque,
                          void* msg_opaque)
{
  luakafka_producer_t* kp = (luakafka_producer_t*)opaque;
  kp->msg_opaque = msg_opaque;
  if (error_code) +kp->failures;
}


static void load_conf(lua_State* lua, rd_kafka_conf_t* conf)
{
  if (!conf) {
    luaL_error(lua, "rd_kafka_conf_new() failed");
  }

  char errstr[512];
  lua_pushnil(lua);
  while (lua_next(lua, 2) != 0) {
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
            r = rd_kafka_conf_set(conf, key, value, errstr, sizeof errstr);
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


static int producer_new(lua_State* lua)
{
  const char* brokers;
  int n = lua_gettop(lua);
  if (n == 1 || n == 2) {
    brokers = luaL_checkstring(lua, 1);
    if (!lua_isnone(lua, 2)) luaL_checktype(lua, 2, LUA_TTABLE);
  } else {
    luaL_argerror(lua, n, "incorrect number of arguments");
  }

  luakafka_producer_t* kp = lua_newuserdata(lua, sizeof(luakafka_producer_t));
  kp->conf = rd_kafka_conf_new();
  if (n == 2) load_conf(lua, kp->conf);
  kp->rk = NULL;
  kp->msg_opaque = NULL;
  kp->failures = 0;
  rd_kafka_conf_set_opaque(kp->conf, kp);

  lua_getfield(lua, LUA_GLOBALSINDEX, async_checkpoint);
  if (lua_type(lua, -1) == LUA_TFUNCTION) {
    rd_kafka_conf_set_dr_cb(kp->conf, msg_delivered);
  }
  lua_pop(lua, 1);

  char errstr[512];
  // create Kafka handle
  kp->rk = rd_kafka_new(RD_KAFKA_PRODUCER, kp->conf, errstr, sizeof errstr);
  if (!kp->rk) {
    return luaL_error(lua, "Failed to create a Kafka producer: %s", errstr);
  }

  // disable logging
  rd_kafka_set_logger(kp->rk, NULL);

  if (rd_kafka_brokers_add(kp->rk, brokers) == 0) {
    return luaL_error(lua, "No valid brokers specified");
  }

  luaL_getmetatable(lua, luakafka_producer);
  lua_setmetatable(lua, -2);
  return 1;
}


static int producer_poll(lua_State* lua)
{
  luakafka_producer_t* kp = check_producer(lua, 1);
  kp->failures = 0;
  kp->msg_opaque = NULL;
  rd_kafka_poll(kp->rk, 0);
  if (kp->msg_opaque) {
    lua_getfield(lua, LUA_GLOBALSINDEX, async_checkpoint);
    if (lua_type(lua, -1) == LUA_TFUNCTION) {
      lua_pushlightuserdata(lua, kp->msg_opaque);
      lua_pushinteger(lua, kp->failures);
      if (lua_pcall(lua, 2, 0, 0)) {
        lua_error(lua);
      }
    }
    lua_pop(lua, 1);
  }
  return 0;
}


static int producer_gc(lua_State* lua)
{
  luakafka_producer_t* kp = check_producer(lua, 1);
  // The topics have already been destroyed don't wait on the outq.
  rd_kafka_destroy(kp->rk);
  // This may timeout because it might not be the last plugin running.
  rd_kafka_wait_destroyed(2000);
  return 0;
}


static const struct luaL_reg producerlib_f[] =
{
  { "new", producer_new },
  { NULL, NULL }
};


static const struct luaL_reg producerlib_m[] =
{
  { "poll", producer_poll },
  { "__gc", producer_gc },
  { NULL, NULL }
};


int luaopen_kafka_producer(lua_State* lua)
{
  luaL_newmetatable(lua, luakafka_producer);
  lua_pushvalue(lua, -1);
  lua_setfield(lua, -2, "__index");
  luaL_register(lua, NULL, producerlib_m);
  luaL_register(lua, lualuakafka_producer_table, producerlib_f);
  return 1;
}
