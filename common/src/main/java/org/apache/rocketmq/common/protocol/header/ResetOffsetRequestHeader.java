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
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

// 即向所有订阅topic@group下的客户端重置为存储时间戳为timestamp的commLogOffset
public class ResetOffsetRequestHeader implements CommandCustomHeader {
    
	@CFNotNull
    private String topic; // topic名称
	
    @CFNotNull
    private String group; // 消费组名称
    
    // 1、-1 : 表示获取group-topic下的ConsumerQueue对象最大偏移量
    // 2、> 0: 表示获取group-topic下的ConsumerQueue对象中在timestamp的偏移量
    @CFNotNull 
    private long timestamp;
    
    // true : timeStampOffset > consumerOffset : 更新timeStampOffset时间戳
    @CFNotNull
    private boolean isForce;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isForce() {
        return isForce;
    }

    public void setForce(boolean isForce) {
        this.isForce = isForce;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
