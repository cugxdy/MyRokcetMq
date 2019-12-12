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

package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;


// 查询指定group-topic下的SubscriptionData是否匹配在index处的count个消息数据
public class QueryConsumeQueueResponseBody extends RemotingSerializable {

	// group-topic下的订阅组对象
    private SubscriptionData subscriptionData;
    
    // 记录topic-group下的ConsumerFilterData字符串对象
    private String filterData;
    
    // ConsumerQueue中20字节存储单元相关状态信息以及MessageFilter匹配结果对象
    private List<ConsumeQueueData> queueData;
    
    // consumeQueue中最大偏移量(单位: 20字节)
    private long maxQueueIndex;
    
    // consumeQueue中最小偏移量(单位: 20字节)
    private long minQueueIndex;

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public void setSubscriptionData(SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    public String getFilterData() {
        return filterData;
    }

    public void setFilterData(String filterData) {
        this.filterData = filterData;
    }

    public List<ConsumeQueueData> getQueueData() {
        return queueData;
    }

    public void setQueueData(List<ConsumeQueueData> queueData) {
        this.queueData = queueData;
    }

    public long getMaxQueueIndex() {
        return maxQueueIndex;
    }

    public void setMaxQueueIndex(long maxQueueIndex) {
        this.maxQueueIndex = maxQueueIndex;
    }

    public long getMinQueueIndex() {
        return minQueueIndex;
    }

    public void setMinQueueIndex(long minQueueIndex) {
        this.minQueueIndex = minQueueIndex;
    }
}
