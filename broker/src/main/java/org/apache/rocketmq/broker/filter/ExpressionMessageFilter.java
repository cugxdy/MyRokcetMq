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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

// 它是用于消息过滤中(GetMessage), 不支持对重试主题的属性过滤
public class ExpressionMessageFilter implements MessageFilter {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    // 订阅组消息
    protected final SubscriptionData subscriptionData;

    // 消费过滤组Data
    protected final ConsumerFilterData consumerFilterData;
    
    // 消息过滤管理对象
    protected final ConsumerFilterManager consumerFilterManager;

    // 布隆过滤器是否有效
    protected final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
        ConsumerFilterManager consumerFilterManager) {
    	
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        
        // consumerFilterData不允许为null
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        
        // 判断manager中的BloomFilter与FilterData中的BoomFilter是否一致性
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        if (bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData())) {
            bloomDataValid = true;
        } else {
            bloomDataValid = false;
        }
    }

    @Override 
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        // 当订阅组为null时,直接就返回true
    	if (null == subscriptionData) {
            return true;
        }

    	// classFilterMode = true: 返回true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // by tags code. // true : tag | null
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {

        	// tagsCode为空或者为<0, 返回true, 说明消息在发送时没有设置TAG
            if (tagsCode == null) {
                return true;
            }

            // 判断是否匹配所有"*"
            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }

            // 判断是否为子集(即包含关系)
            return subscriptionData.getCodeSet().contains(tagsCode.intValue());
        } else {
        	
            // no expression or no bloom
        	// 当CompiledExpression与BloomFilter存在为空时,返回true
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }

            // message is before consumer
            // 判断message是否在消费之前生成
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }

            // 获取ConsumeQueue中扩展的属性BitMap属性
            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            
            // 判断是否符合特定需求(比特位长度是否一致)
            if (filterBitMap == null || !this.bloomDataValid
                || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }

            BitsArray bitsArray = null;
            try {
            	// 创建BitsArray对象
                bitsArray = BitsArray.create(filterBitMap);
                
                // 布隆过滤器去判断true/false
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData
                    + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }

        return true;
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        // 订阅组为null,返回true
    	if (subscriptionData == null) {
            return true;
        }

    	// true : classFilterMode模式
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // true : tag | null
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }

        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;

        // no expression
        // 当expression为null时,返回true
        if (realFilterData == null || realFilterData.getExpression() == null
            || realFilterData.getCompiledExpression() == null) {
            return true;
        }

        if (tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
        	// 创建Context对象
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            // 使用JAVA CC编译器去编译context对象
            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }

        // 返回执行结果对象
        return (Boolean) ret;
    }

}
