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
package org.apache.rocketmq.broker.offset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 它记录某个group-topic-queueId的consumeQueue消费索引计数
public class ConsumerOffsetManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    // topic与group分隔符
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable =
        new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>(512);

    private transient BrokerController brokerController;

    public ConsumerOffsetManager() {
    }

    // 创建ConsumerOffsetManager对象
    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    // 删除SubscriptionData = null & offsetInPersist <= minOffsetInStore的topic@group对象
    public void scanUnsubscribedTopic() {
    	
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            
            // topic@group字符串
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];

                // SubscriptionData = null && offsetInPersist <= minOffsetInStore
                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic)
                    && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    // 删除这个元素
                	it.remove();
                    log.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    // 判断topic-group下是否存在记录索引  < 实际consumeQueue中索引
    // true : 表示所有的offsetInPersist <= minOffsetInStore;
    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();
        
        // 判断topic-queueId下的offsetInPersist是否小于实际上的consumeQueue中的最小偏移量(单位:20字节)
        while (it.hasNext() && result) {
        	
            Entry<Integer, Long> next = it.next();
            // 获取topic-queueId下的consumeQueue中最小偏移量(单位:20字节)
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, next.getKey());
            long offsetInPersist = next.getValue();
            
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    // 获取group所持有的Topic
    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<String>();

        // 获取offsetTable对象的索引号
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            // topic@group字符串
            String topicAtGroup = next.getKey();
            // 以@作为分隔符分隔
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
            	// 判断参数topic与Map集合中group是否一致
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }

    // 获取topic下所有的group集合对象
    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<String>();

        // 获取offsetTable对象的迭代器
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            // 获取topic@group字符串
            String topicAtGroup = next.getKey();
            // 以@作为分隔符分隔
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
            	// 判断参数topic与Map集合中topic是否一致
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }

        return groups;
    }

    // 更新offsetTable中的偏移量
    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }

    // 更新客户端在topic@group-queueId-Offset下的消费偏移量
    private void commitOffset(final String clientHost, final String topicGroup, final int queueId, final long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(topicGroup);
        if (null == map) {
        	// 更新偏移量
            map = new ConcurrentHashMap<Integer, Long>(32);
            map.put(queueId, offset);
            this.offsetTable.put(topicGroup, map);
        } else {
        	// 存储偏移量(记录topic-queueId下的索引)
            Long storeOffset = map.put(queueId, offset);
            // 当offset小于storeOffset,可能出现重复消费
            if (storeOffset != null && offset < storeOffset) {
                log.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, topicGroup, queueId, offset, storeOffset);
            }
        }
    }

    // 查询topic@group下指定queueId的索引
    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
        	// 获取指定topic-Group-queueId下索引号
            Long offset = map.get(queueId);
            if (offset != null)
                return offset;
        }

        return -1;
    }

    // 编码
    public String encode() {
        return this.encode(false);
    }

    @Override // 获取配置文件路径
    public String configFilePath() {
    	// config/consumerOffset.json
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
        	// 使用JSON将字符串解析成ConsumerOffsetManager对象
        	// jsonString格式为:{key:{key:value}}
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }

    // 将this对象编码成jsonString字符串
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    // 获取偏移量对象
    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    // 设置偏移量对象
    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    // 当ConsumerOffsetManager中偏移量大于MessageStore对象中的偏移量时
    // 获取每个队列的最小偏移量集合Map对象
    // filterGroups: 没什么作用
    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {

        Map<Integer, Long> queueMinOffset = new HashMap<Integer, Long>();
        // 获取topicGroup字符串
        Set<String> topicGroups = this.offsetTable.keySet();
        // 判断是否为空
        if (!UtilAll.isBlank(filterGroups)) {
        	// 过滤掉指定filterGroups集合,即filterGroups指定的group不需要
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    if (group.equals(it.next().split(TOPIC_GROUP_SEPARATOR)[1])) {
                        it.remove();
                    }
                }
            }
        }

        
        for (Map.Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            // 获取topicGroup字符串
        	String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            
            // 当指定topic相等时
            if (topic.equals(topicGroupArr[0])) {
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                	// 获取topic-queueId下的consumeQueue中最小偏移量(单位:20字节)
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, entry.getKey());
                    // 当ConsumerOffsetManager中偏移量大于MessageStore对象consumeQueue中的偏移量时
                    if (entry.getValue() >= minOffset) {
                    	// value有效时
                        Long offset = queueMinOffset.get(entry.getKey());
                        
                        // 将key-value键值对存入queueMinOffset对象中
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        } else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }
        }
        return queueMinOffset;
    }

    // 查询topic@group下所有的偏移量Map对象
    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    // clone至另一个group对象中去
    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<Integer, Long>(offsets));
        }
    }

}
