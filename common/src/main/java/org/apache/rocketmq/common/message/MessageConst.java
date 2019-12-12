/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.message;

import java.util.HashSet;

// 它记录着Message对象中常用properties中属性值
public class MessageConst {
	// 它用于去记录消息的KEYS属性值
    public static final String PROPERTY_KEYS = "KEYS";
    // 它用于去记录消息的Tags属性值
    public static final String PROPERTY_TAGS = "TAGS";
    
    // 是否消息存储等待, 它使用于 store中主从同步|磁盘刷新
    public static final String PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
    
    // 它用于去记录消息的延迟级别!
    // 例如 : 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
    public static final String PROPERTY_DELAY_TIME_LEVEL = "DELAY";
    
    // 它用于记录消息重试topic主题下的原topic(Key对象)
    public static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
    
    // 在延迟消息中, 它用于记录消息真实topic主题
    public static final String PROPERTY_REAL_TOPIC = "REAL_TOPIC";
    // 在延迟消息中, 它用于记录消息真实QueueId
    public static final String PROPERTY_REAL_QUEUE_ID = "REAL_QID";
    
    // 它是消息属性中记录事务消息的Key对象
    public static final String PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";
    // 它是消息属性中记录生成者组名称的key对象(它是用于在事务消息中)
    public static final String PROPERTY_PRODUCER_GROUP = "PGROUP";
    
    // 它是消息属性中记录MinOffset的key对象(消息所在consumeQueue最小索引)
    public static final String PROPERTY_MIN_OFFSET = "MIN_OFFSET";
    // 它是消息属性中记录MAX_OFFSET的key对象(消息所在consumeQueue最大索引)
    public static final String PROPERTY_MAX_OFFSET = "MAX_OFFSET";
    
    public static final String PROPERTY_BUYER_ID = "BUYER_ID";
    
    // 原始消息ID, sendMessageBack, newTopic消息ORIGIN_MESSAGE_ID属性值
    public static final String PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
    
    
    public static final String PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
    
    public static final String PROPERTY_CORRECTION_FLAG = "CORRECTION_FLAG";
    
    public static final String PROPERTY_MQ2_FLAG = "MQ2_FLAG";
    
    // 消息重新消费次数
    public static final String PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
    
    public static final String PROPERTY_MSG_REGION = "MSG_REGION";
    
    public static final String PROPERTY_TRACE_SWITCH = "TRACE_ON";
    
    
    // 消息的唯一标识(基于时间戳与计数器实现)
    public static final String PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
    
    // 消息最大重新消费次数
    public static final String PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";
    
    // 消息消费开始时间
    public static final String PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";

    // 空格分隔符
    public static final String KEY_SEPARATOR = " ";

    public static final HashSet<String> STRING_HASH_SET = new HashSet<String>();

    static {
        STRING_HASH_SET.add(PROPERTY_TRACE_SWITCH);
        STRING_HASH_SET.add(PROPERTY_MSG_REGION);
        STRING_HASH_SET.add(PROPERTY_KEYS);
        STRING_HASH_SET.add(PROPERTY_TAGS);
        STRING_HASH_SET.add(PROPERTY_WAIT_STORE_MSG_OK);
        STRING_HASH_SET.add(PROPERTY_DELAY_TIME_LEVEL);
        STRING_HASH_SET.add(PROPERTY_RETRY_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_QUEUE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED);
        STRING_HASH_SET.add(PROPERTY_PRODUCER_GROUP);
        STRING_HASH_SET.add(PROPERTY_MIN_OFFSET);
        STRING_HASH_SET.add(PROPERTY_MAX_OFFSET);
        STRING_HASH_SET.add(PROPERTY_BUYER_ID);
        STRING_HASH_SET.add(PROPERTY_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSFER_FLAG);
        STRING_HASH_SET.add(PROPERTY_CORRECTION_FLAG);
        STRING_HASH_SET.add(PROPERTY_MQ2_FLAG);
        STRING_HASH_SET.add(PROPERTY_RECONSUME_TIME);
        STRING_HASH_SET.add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        STRING_HASH_SET.add(PROPERTY_MAX_RECONSUME_TIMES);
        STRING_HASH_SET.add(PROPERTY_CONSUME_START_TIMESTAMP);
    }
}
