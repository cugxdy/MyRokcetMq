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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.DispatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

/**
 * Calculate bit map of filter.
 */
public class CommitLogDispatcherCalcBitMap implements CommitLogDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    
    // 记录ConsumerFilterData对象
    protected final ConsumerFilterManager consumerFilterManager;

    public CommitLogDispatcherCalcBitMap(BrokerConfig brokerConfig, ConsumerFilterManager consumerFilterManager) {
        this.brokerConfig = brokerConfig;
        this.consumerFilterManager = consumerFilterManager;
    }

    @Override
    public void dispatch(DispatchRequest request) {
    	// default = false
        if (!this.brokerConfig.isEnableCalcFilterBitMap()) {
            return;
        }

        try {
        	// 获取topic下的ConsumerFilterData对象集合
            Collection<ConsumerFilterData> filterDatas = consumerFilterManager.get(request.getTopic());

            if (filterDatas == null || filterDatas.isEmpty()) {
                return;
            }

            Iterator<ConsumerFilterData> iterator = filterDatas.iterator();
            // 创建BitsArray对象(byte[]字节数组)
            BitsArray filterBitMap = BitsArray.create(
                this.consumerFilterManager.getBloomFilter().getM()
            );

            long startTime = System.currentTimeMillis();
            while (iterator.hasNext()) {
                ConsumerFilterData filterData = iterator.next();

                // expression == null
                if (filterData.getCompiledExpression() == null) {
                    log.error("[BUG] Consumer in filter manager has no compiled expression! {}", filterData);
                    continue;
                }

                // BloomFilterData == null
                if (filterData.getBloomFilterData() == null) {
                    log.error("[BUG] Consumer in filter manager has no bloom data! {}", filterData);
                    continue;
                }

                Object ret = null;
                try {
                    MessageEvaluationContext context = new MessageEvaluationContext(request.getPropertiesMap());

                    // JAVA cc编译字符串
                    ret = filterData.getCompiledExpression().evaluate(context);
                } catch (Throwable e) {
                    log.error("Calc filter bit map error!commitLogOffset={}, consumer={}, {}", request.getCommitLogOffset(), filterData, e);
                }

                log.debug("Result of Calc bit map:ret={}, data={}, props={}, offset={}", ret, filterData, request.getPropertiesMap(), request.getCommitLogOffset());

                // eval true 
                if (ret != null && ret instanceof Boolean && (Boolean) ret) {
                    
                	// 将ConsumerFilterData中的int[]数据写入到BitsArray中
                	consumerFilterManager.getBloomFilter().hashTo(
                        filterData.getBloomFilterData(),
                        filterBitMap
                    );
                }
            }

            // 将BitsArray对象byte[]数据设置至DispatchRequest对象中
            // 最终设置ConsumeQueueExt.bitmap: byte[] 字节数组上
            request.setBitMap(filterBitMap.bytes());

            long eclipseTime = System.currentTimeMillis() - startTime;
            // 1ms 记录设置BitMap所用时间戳
            if (eclipseTime >= 1) {
                log.warn("Spend {} ms to calc bit map, consumerNum={}, topic={}", eclipseTime, filterDatas.size(), request.getTopic());
            }
        } catch (Throwable e) {
            log.error("Calc bit map error! topic={}, offset={}, queueId={}, {}", request.getTopic(), request.getCommitLogOffset(), request.getQueueId(), e);
        }
    }
}
