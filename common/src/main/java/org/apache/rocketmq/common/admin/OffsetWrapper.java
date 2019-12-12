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
package org.apache.rocketmq.common.admin;

// 偏移量统计类, 用于Broker服务器消费状态
public class OffsetWrapper {
	
	// 获取在consumeQueue最大物理偏移量(单位:CQ_STORE_UNIT_SIZE)
    private long brokerOffset;
    
    // 获取指定group@topic-queueId下的offset对象(用户消费的偏移量)
    private long consumerOffset;

    // 获取最近消费的消息对象在commitLog中的存储时间戳
    private long lastTimestamp;

    public long getBrokerOffset() {
        return brokerOffset;
    }

    public void setBrokerOffset(long brokerOffset) {
        this.brokerOffset = brokerOffset;
    }

    public long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }
}
