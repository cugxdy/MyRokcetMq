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

import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer filter data manager.Just manage the consumers use expression filter.
 */
public class ConsumerFilterManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    // 1天
    private static final long MS_24_HOUR = 24 * 3600 * 1000;

    // Topic与FilterDataMapByTopic之间的一对一关系
    private ConcurrentMap<String/*Topic*/, FilterDataMapByTopic>
        filterDataByTopic = new ConcurrentHashMap<String/*consumer group*/, FilterDataMapByTopic>(256);

    private transient BrokerController brokerController;
    // 布隆过滤器
    private transient BloomFilter bloomFilter;

    public ConsumerFilterManager() {
        // just for test
        this.bloomFilter = BloomFilter.createByFn(20, 64);
    }

    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        
        this.bloomFilter = BloomFilter.createByFn(
            brokerController.getBrokerConfig().getMaxErrorRateOfBloomFilter(),
            brokerController.getBrokerConfig().getExpectConsumerNumUseFilter()
        );
        
        // then set bit map length of store config.
        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(
            this.bloomFilter.getM()
        );
    }

    /**
     * Build consumer filter data.Be care, bloom filter data is not included.
     *
     * @return maybe null
     */
    // 创建ConsumerFilterData对象
    public static ConsumerFilterData build(final String topic, final String consumerGroup,
        final String expression, final String type,
        final long clientVersion) {
    	
    	// 判断是否为"TAG"类型
        if (ExpressionType.isTagType(type)) {
            return null;
        }

        ConsumerFilterData consumerFilterData = new ConsumerFilterData();
        
        // 设置相应属性值
        consumerFilterData.setTopic(topic);
        consumerFilterData.setConsumerGroup(consumerGroup);
        consumerFilterData.setBornTime(System.currentTimeMillis());
        consumerFilterData.setDeadTime(0);
        
        consumerFilterData.setExpression(expression);
        consumerFilterData.setExpressionType(type);
        // 设置客户端版本号
        consumerFilterData.setClientVersion(clientVersion);
        
        try {
        	
        	// 编译表达式
            consumerFilterData.setCompiledExpression(
                FilterFactory.INSTANCE.get(type).compile(expression)
            );
        } catch (Throwable e) {
            log.error("parse error: expr={}, topic={}, group={}, error={}", expression, topic, consumerGroup, e.getMessage());
            return null;
        }

        return consumerFilterData;
    }

    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            register(
                subscriptionData.getTopic(),
                consumerGroup,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion()
            );
        }

        // make illegal topic dead.
        Collection<ConsumerFilterData> groupFilterData = getByGroup(consumerGroup);

        Iterator<ConsumerFilterData> iterator = groupFilterData.iterator();
        while (iterator.hasNext()) {
            ConsumerFilterData filterData = iterator.next();

            boolean exist = false;
            for (SubscriptionData subscriptionData : subList) {
                if (subscriptionData.getTopic().equals(filterData.getTopic())) {
                    exist = true;
                    break;
                }
            }

            if (!exist && !filterData.isDead()) {
                filterData.setDeadTime(System.currentTimeMillis());
                log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
            }
        }
    }

    // 注册FilterDataMapByTopic对象
    public boolean register(final String topic, final String consumerGroup, final String expression,
        final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return false;
        }

        if (expression == null || expression.length() == 0) {
            return false;
        }

        FilterDataMapByTopic filterDataMapByTopic = this.filterDataByTopic.get(topic);

        if (filterDataMapByTopic == null) {
            FilterDataMapByTopic temp = new FilterDataMapByTopic(topic);
            FilterDataMapByTopic prev = this.filterDataByTopic.putIfAbsent(topic, temp);
            filterDataMapByTopic = prev != null ? prev : temp;
        }

        // 创建BloomFilterData对象
        BloomFilterData bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);

        return filterDataMapByTopic.register(consumerGroup, expression, type, bloomFilterData, clientVersion);
    }

    // 注销消费组对象
    public void unRegister(final String consumerGroup) {
        for (String topic : filterDataByTopic.keySet()) {
            this.filterDataByTopic.get(topic).unRegister(consumerGroup);
        }
    }

    // 获取指定Topic与消费组consumerGroup下的ConsumerFilterData对象
    public ConsumerFilterData get(final String topic, final String consumerGroup) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().get(consumerGroup);
    }

    // 获取指定消费组下ConsumerFilterData对象集合
    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<ConsumerFilterData>();

        Iterator<FilterDataMapByTopic> topicIterator = this.filterDataByTopic.values().iterator();
        while (topicIterator.hasNext()) {
            FilterDataMapByTopic filterDataMapByTopic = topicIterator.next();

            Iterator<ConsumerFilterData> filterDataIterator = filterDataMapByTopic.getGroupFilterData().values().iterator();

            while (filterDataIterator.hasNext()) {
                ConsumerFilterData filterData = filterDataIterator.next();

                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }

        return ret;
    }

    // 获取ConsumerFilterData对象集合
    public final Collection<ConsumerFilterData> get(final String topic) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().values();
    }

    // 获取BloomFilter对象
    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override// 获取存储路径
    public String configFilePath() {
        if (this.brokerController != null) {
            return BrokerPathConfigHelper.getConsumerFilterPath(
                this.brokerController.getMessageStoreConfig().getStorePathRootDir()
            );
        }
        return BrokerPathConfigHelper.getConsumerFilterPath("./unit_test");
    }

    @Override// 对字符串进行解码成ConsumerFilterManager对象
    public void decode(final String jsonString) {
        ConsumerFilterManager load = RemotingSerializable.fromJson(jsonString, ConsumerFilterManager.class);
        if (load != null && load.filterDataByTopic != null) {
            boolean bloomChanged = false;
            // 遍历Topic对象
            for (String topic : load.filterDataByTopic.keySet()) {
                FilterDataMapByTopic dataMapByTopic = load.filterDataByTopic.get(topic);
                if (dataMapByTopic == null) {
                    continue;
                }

                // 遍历消费组名称
                for (String group : dataMapByTopic.getGroupFilterData().keySet()) {

                	// 获取ConsumerFilterData对象
                    ConsumerFilterData filterData = dataMapByTopic.getGroupFilterData().get(group);

                    if (filterData == null) {
                        continue;
                    }

                    try {
                    	// 遍历表达式
                        filterData.setCompiledExpression(
                            FilterFactory.INSTANCE.get(filterData.getExpressionType()).compile(filterData.getExpression())
                        );
                    } catch (Exception e) {
                        log.error("load filter data error, " + filterData, e);
                    }

                    // check whether bloom filter is changed
                    // if changed, ignore the bit map calculated before.
                    // 验证布隆过滤器是否change
                    if (!this.bloomFilter.isValid(filterData.getBloomFilterData())) {
                        bloomChanged = true; // change = true
                        log.info("Bloom filter is changed!So ignore all filter data persisted! {}, {}", this.bloomFilter, filterData.getBloomFilterData());
                        break;
                    }

                    log.info("load exist consumer filter data: {}", filterData);

                    // 设置死亡时间
                    if (filterData.getDeadTime() == 0) {
                        // we think all consumers are dead when load
                        long deadTime = System.currentTimeMillis() - 30 * 1000;
                        filterData.setDeadTime(
                            deadTime <= filterData.getBornTime() ? filterData.getBornTime() : deadTime
                        );
                    }
                }
            }

            // 如果布隆过滤器更改的话,重新设置filterDataByTopic属性值
            if (!bloomChanged) {
                this.filterDataByTopic = load.filterDataByTopic;
            }
        }
    }

    @Override// 编码该对象
    public String encode(final boolean prettyFormat) {
        // clean
        {
            clean();
        }
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    // 擦除已经死亡指定时间的对象
    public void clean() {
        Iterator<Map.Entry<String, FilterDataMapByTopic>> topicIterator = this.filterDataByTopic.entrySet().iterator();
        while (topicIterator.hasNext()) {
            Map.Entry<String, FilterDataMapByTopic> filterDataMapByTopic = topicIterator.next();

            Iterator<Map.Entry<String, ConsumerFilterData>> filterDataIterator
                = filterDataMapByTopic.getValue().getGroupFilterData().entrySet().iterator();

            while (filterDataIterator.hasNext()) {
                Map.Entry<String, ConsumerFilterData> filterDataByGroup = filterDataIterator.next();

                ConsumerFilterData filterData = filterDataByGroup.getValue();
                // 当死亡时间大于指定时间时,就将ConsumerFilterData对应关系删除掉
                if (filterData.howLongAfterDeath() >= (this.brokerController == null ? MS_24_HOUR : this.brokerController.getBrokerConfig().getFilterDataCleanTimeSpan())) {
                    log.info("Remove filter consumer {}, died too long!", filterDataByGroup.getValue());
                    filterDataIterator.remove();
                }
            }

            // 如果Topic下的FilterDataMapByTopic为空,直接删除映射关系
            if (filterDataMapByTopic.getValue().getGroupFilterData().isEmpty()) {
                log.info("Topic has no consumer, remove it! {}", filterDataMapByTopic.getKey());
                topicIterator.remove();
            }
        }
    }

    // 获取filterDataByTopic属性值
    public ConcurrentMap<String, FilterDataMapByTopic> getFilterDataByTopic() {
        return filterDataByTopic;
    }

    // 设置filterDataByTopic属性值
    public void setFilterDataByTopic(final ConcurrentHashMap<String, FilterDataMapByTopic> filterDataByTopic) {
        this.filterDataByTopic = filterDataByTopic;
    }

    public static class FilterDataMapByTopic {

    	// 消费组与ConsumerFilterData之间的一对一关系
        private ConcurrentMap<String/*consumer group*/, ConsumerFilterData>
            groupFilterData = new ConcurrentHashMap<String, ConsumerFilterData>();

        // Topic名称
        private String topic;

        public FilterDataMapByTopic() {
        }

        public FilterDataMapByTopic(String topic) {
            this.topic = topic;
        }

        // 注销ConsumerFilterData对象
        public void unRegister(String consumerGroup) {
        	// 判断是否存在该Map对象中
            if (!this.groupFilterData.containsKey(consumerGroup)) {
                return;
            }

            ConsumerFilterData data = this.groupFilterData.get(consumerGroup);

            // 如果为空或者已经处于死亡状态,直接返回
            if (data == null || data.isDead()) {
                return;
            }

            // 获取当前系统时间
            long now = System.currentTimeMillis();

            log.info("Unregister consumer filter: {}, deadTime: {}", data, now);

            // 设置死亡时间
            data.setDeadTime(now);
        }

        // 注册ConsumerFilterData对象
        public boolean register(String consumerGroup, String expression, String type, BloomFilterData bloomFilterData,
            long clientVersion) {
        	
            ConsumerFilterData old = this.groupFilterData.get(consumerGroup);

            if (old == null) {
            	// 创建ConsumerFilterData对象
                ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                if (consumerFilterData == null) {
                    return false;
                }
                
                // 设置BloomFilterData对象
                consumerFilterData.setBloomFilterData(bloomFilterData);

                old = this.groupFilterData.putIfAbsent(consumerGroup, consumerFilterData);
                
                if (old == null) {
                    log.info("New consumer filter registered: {}", consumerFilterData);
                    return true;
                } else {
                	// 当小于先前的ClientVersion版本时
                    if (clientVersion <= old.getClientVersion()) {
                    	
                        if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                            log.warn("Ignore consumer({} : {}) filter(concurrent), because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                consumerGroup, topic,
                                clientVersion, old.getClientVersion(),
                                old.getExpressionType(), old.getExpression(),
                                type, expression);
                        }
                        if (clientVersion == old.getClientVersion() && old.isDead()) {
                        	// 复活该对象
                            reAlive(old);
                            return true;
                        }

                        return false;
                    } else {
                    	// 将先前存在的对象直接替换
                        this.groupFilterData.put(consumerGroup, consumerFilterData);
                        log.info("New consumer filter registered(concurrent): {}, old: {}", consumerFilterData, old);
                        return true;
                    }
                }
            } else {
                if (clientVersion <= old.getClientVersion()) {
                    if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                        log.info("Ignore consumer({}:{}) filter, because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                            consumerGroup, topic,
                            clientVersion, old.getClientVersion(),
                            old.getExpressionType(), old.getExpression(),
                            type, expression);
                    }
                    if (clientVersion == old.getClientVersion() && old.isDead()) {
                        // 复活对象
                    	reAlive(old);
                        return true;
                    }

                    return false;
                }

                // 判断expression与type是否发生变化
                boolean change = !old.getExpression().equals(expression) || !old.getExpressionType().equals(type);
                if (old.getBloomFilterData() == null && bloomFilterData != null) {
                    change = true;
                }
                if (old.getBloomFilterData() != null && !old.getBloomFilterData().equals(bloomFilterData)) {
                    change = true;
                }

                // if subscribe data is changed, or consumer is died too long.
                // 对象属性值发生变化
                if (change) {
                	// 创建ConsumerFilterData对象
                    ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                    if (consumerFilterData == null) {
                        // new expression compile error, remove old, let client report error.
                        // 删除对象
                    	this.groupFilterData.remove(consumerGroup);
                        return false;
                    }
                    consumerFilterData.setBloomFilterData(bloomFilterData);

                    // 替换先前的对象
                    this.groupFilterData.put(consumerGroup, consumerFilterData);

                    log.info("Consumer filter info change, old: {}, new: {}, change: {}",
                        old, consumerFilterData, change);

                    return true;
                } else {
                	// 设置版本号并复活对象
                    old.setClientVersion(clientVersion);
                    if (old.isDead()) {
                        reAlive(old); // 复活
                    }
                    return true;
                }
            }
        }

        // 复活ConsumerFilterData对象
        protected void reAlive(ConsumerFilterData filterData) {
            long oldDeadTime = filterData.getDeadTime();
            filterData.setDeadTime(0);
            log.info("Re alive consumer filter: {}, oldDeadTime: {}", filterData, oldDeadTime);
        }

        // 获取ConsumerFilterData对象
        public final ConsumerFilterData get(String consumerGroup) {
            return this.groupFilterData.get(consumerGroup);
        }

        // 获取映射关系对象
        public final ConcurrentMap<String, ConsumerFilterData> getGroupFilterData() {
            return this.groupFilterData;
        }

        // 设置映射关系对象
        public void setGroupFilterData(final ConcurrentHashMap<String, ConsumerFilterData> groupFilterData) {
            this.groupFilterData = groupFilterData;
        }

        // 获取Topic名称
        public String getTopic() {
            return topic;
        }

        public void setTopic(final String topic) {
            this.topic = topic;
        }
    }
}
