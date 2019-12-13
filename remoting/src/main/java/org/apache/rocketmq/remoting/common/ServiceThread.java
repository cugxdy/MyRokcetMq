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
package org.apache.rocketmq.remoting.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for background thread
 */
public abstract class ServiceThread implements Runnable {
	// 日志记录器
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    // join执行时间90s
    private static final long JOIN_TIME = 90 * 1000;
    // 线程对象
    protected final Thread thread;
    protected volatile boolean hasNotified = false;
    // 是否为stop状态
    protected volatile boolean stopped = false;

    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }

    public abstract String getServiceName();

    // 启动线程对象(Thread)
    public void start() {
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
        	// 唤醒阻塞线程
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }

        try {
        	// 是否触发中断机制
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            // 给与90s时间使得thread对象执行相应任务
            this.thread.join(this.getJointime());
            long eclipseTime = System.currentTimeMillis() - beginTime;
            // 记录该线程执行时间
            log.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    // 获取JOIN_TIME属性值
    public long getJointime() {
        return JOIN_TIME;
    }

    // 获取Thread状态
    public boolean isStopped() {
        return stopped;
    }
}
