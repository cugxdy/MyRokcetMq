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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Support filter to retry topic.
 * <br>It will decode properties first in order to get real topic.
 */
// 支持对重试主题的过滤
public class ExpressionForRetryMessageFilter extends ExpressionMessageFilter {
    
	// 创建ExpressionForRetryMessageFilter对象
	public ExpressionForRetryMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
        ConsumerFilterManager consumerFilterManager) {
        super(subscriptionData, consumerFilterData, consumerFilterManager);
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        // true : 订阅组为null
    	if (subscriptionData == null) {
            return true;
        }

    	// classFilterMode = true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // 判断topic是否以RETRY为前缀
        boolean isRetryTopic = subscriptionData.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);

        // true : tag | null
        if (!isRetryTopic && ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }

        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;
        boolean decoded = false;
        if (isRetryTopic) {
            // retry topic, use original filter data.
            // poor performance to support retry filter.
            if (tempProperties == null && msgBuffer != null) {
                decoded = true;
                tempProperties = MessageDecoder.decodeProperties(msgBuffer);
            }
            // 获取topic字符串
            String realTopic = tempProperties.get(MessageConst.PROPERTY_RETRY_TOPIC);
            // 获取group字符串
            String group = subscriptionData.getTopic().substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            
            // 获取ConsumerFilterData对象
            realFilterData = this.consumerFilterManager.get(realTopic, group);
        }

        // no expression
        // true : expression == null
        if (realFilterData == null || realFilterData.getExpression() == null
            || realFilterData.getCompiledExpression() == null) {
            return true;
        }

        // 获取message中的properties属性Map对象
        if (!decoded && tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            // 使用JAVA CC编译表达式
            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }

        return (Boolean) ret;
    }
}
