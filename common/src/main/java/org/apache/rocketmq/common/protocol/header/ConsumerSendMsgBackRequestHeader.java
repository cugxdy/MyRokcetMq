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

package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

// 将消费端长久未消费的消息重新写回Broker服务器中
public class ConsumerSendMsgBackRequestHeader implements CommandCustomHeader {
	
    @CFNotNull
    private Long offset; // commitLogOffset
    
    @CFNotNull
    private String group; // 消费组名称
    
    @CFNotNull
    private Integer delayLevel; // 延迟级别 default = 3 | 0
    
    private String originMsgId; // 即body中消息MsgId
    
    private String originTopic; // 即body中消息topic
    
    @CFNullable
    private boolean unitMode = false;
    
    // 最大重复消费次数
    private Integer maxReconsumeTimes; // default = 16

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Integer getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(Integer delayLevel) {
        this.delayLevel = delayLevel;
    }

    public String getOriginMsgId() {
        return originMsgId;
    }

    public void setOriginMsgId(String originMsgId) {
        this.originMsgId = originMsgId;
    }

    public String getOriginTopic() {
        return originTopic;
    }

    public void setOriginTopic(String originTopic) {
        this.originTopic = originTopic;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    @Override
    public String toString() {
        return "ConsumerSendMsgBackRequestHeader [group=" + group + ", originTopic=" + originTopic + ", originMsgId=" + originMsgId
            + ", delayLevel=" + delayLevel + ", unitMode=" + unitMode + ", maxReconsumeTimes=" + maxReconsumeTimes + "]";
    }
}
