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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    // topic与queueId字符串分隔符"@"
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    
    private final BrokerController brokerController;
    
    // 系统时钟
    private final SystemClock systemClock = new SystemClock();
    
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        // 创建ManyPullRequest对象
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        // 将pullRequest对象加入List对象
        mpr.addPullRequest(pullRequest);
    }

    // 创建topic@queueId字符串
    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
    	// PullRequestHoldService服务启动
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
            	// true : 开启MessageArrivingListener接口方法
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000); // 间歇性为5s
                } else {
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                // 检查已到达消息(message)
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                // 日志记录消耗时间
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        // PullRequestHoldService服务 shutdown中
        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    // 检查已到达消息(message)
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                // 获取topic与queueId下的consumeQueue下的最大偏移量(单位:20字节)
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                	
                	// 通知消息已到达
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        // 获取topic@queueId字符串对象
    	String key = this.buildKey(topic, queueId);
    	
    	// 获取ManyPullRequest对象
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
        	
        	// 获取PullRequet对象,并将ManyPullRequest对象中的List清空
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                	
                	// 记录topic与queueId下的consumeQueue下的最大偏移量(单位: 20字节)
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    // true : 新的消息已达到时
                    if (newestOffset > request.getPullFromThisOffset()) {
                    	// 判断是否匹配consumeQueue对象
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                        	// 判断是否匹配commitLog对象
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                            	// 执行Channel对象执行Pull请求(拉取)
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }

                    // 判断规定的暂停时间已到达,即向客户端发送message
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                        	// 执行Channel对象执行Pull请求(拉取)
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
