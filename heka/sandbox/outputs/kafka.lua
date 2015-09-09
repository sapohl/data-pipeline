-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "kafka.producer"
require "kafka.topic"

--[[

*Example Configuration*

    filename               = "kafka.lua"
    message_matcher        = "TRUE"
    output_limit           = 8 * 1024 * 1024
    brokers                = "localhost:9092"
    ticker_interval        = 60
    async_buffer_size      = 20000

    topic_constant = "test"
    producer_conf = {
        ["queue.buffering.max.messages"] = async_buffer_size,
        ["batch.num.messages"] = 200,
        ["message.max.bytes"] = output_limit,
        ["queue.buffering.max.ms"] = 10,
        ["topic.metadata.refresh.interval.ms"] = -1,
    }

--]]
local brokers = read_config("brokers") or error("brokers must be set")
local topic_constant = read_config("topic_constant")
local topic_variable = read_config("topic_variable") or "Logger"
local producer_conf = read_config("producer_conf")

local producer = kafka.producer.new(brokers, producer_conf)
local topics = {}

function process_message(sequence_id)
    local var = topic_constant
    if not var then
        var = read_message(topic_variable) or "unknown"
    end

    local topic = topics[var]
    if not topic then
        topic = kafka.topic.new(producer, var)
        topics[var] = topic
    end

    producer:poll()
    local ret = topic:send(0, read_message("raw"), sequence_id)

    if ret ~= 0 then
        if ret == 105 then
            return -4 -- queue full retry
        elseif ret == 90 then
            return -1 -- message too large
        elseif ret == 2 then
            error("unknown topic: " .. var)
        elseif ret == 3 then
            error("unknown partition")
        end
    end

    return -5 -- asynchronous checkpoint management
end

function timer_event(ns)
    producer:poll()
end
