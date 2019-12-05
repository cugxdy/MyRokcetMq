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
package org.apache.rocketmq.broker.latency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BrokerFastFailure will cover {@link BrokerController#sendThreadPoolQueue} and
 * {@link BrokerController#pullThreadPoolQueue}
 */
public class BrokerFastFailure {
	
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    // 定时任务线程池
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "BrokerFastFailureScheduledThread"));
    
    // Broker启动控制器
    private final BrokerController brokerController;

    
    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    // 获取RequestTask对象
    public static RequestTask castRunnable(final Runnable runnable) {
        try {
            FutureTaskExt<?> object = (FutureTaskExt<?>) runnable;
            return (RequestTask) object.getRunnable();
        } catch (Throwable e) {
            log.error(String.format("castRunnable exception, %s", runnable.getClass().getName()), e);
        }

        return null;
    }

    // Broker启动选项
    public void start() {
    	// 定时任务, 1s启动, 10ms周期
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
            	// default = true
                if (brokerController.getBrokerConfig().isBrokerFastFailureEnable()) {
                    cleanExpiredRequest();
                }
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    private void cleanExpiredRequest() {
    	// 当前系统处于繁忙状态, lockDiff > 1000ms
        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
            	
            	// 判断发送线程池任务队列是否为空
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
                	// 获取线程池任务
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                    if (null == runnable) {
                        break;
                    }

                    // 获取RequestTask对象
                    final RequestTask rt = castRunnable(runnable);
                    
                    // 设置系统繁忙, 向客户端返回系统繁忙, 暂时不支持消息Pull拉取
                    rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, 
                    		String.format("[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, "
                    		+ "period in queue: %sms, size of queue: %d", 
                    		System.currentTimeMillis() - rt.getCreateTimestamp(), 
                    		this.brokerController.getSendThreadPoolQueue().size()));
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }

        cleanExpiredRequestInQueue(this.brokerController.getSendThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInSendQueue());

        cleanExpiredRequestInQueue(this.brokerController.getPullThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInPullQueue());
    }

    void cleanExpiredRequestInQueue(final BlockingQueue<Runnable> blockingQueue, final long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
            	// 判断阻塞队列是否为空
                if (!blockingQueue.isEmpty()) {
                	// 从blockingQueue中获取任务队列但并删除任务
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    
                    // 获取RequestTask任务
                    final RequestTask rt = castRunnable(runnable);
                    // RequestTask运行任务是否停止
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    // 判断是否达到最大时间
                    if (behind >= maxWaitTimeMillsInQueue) {
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            // 设置系统繁忙
                            rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    // shutdown 定时任务线程池
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
