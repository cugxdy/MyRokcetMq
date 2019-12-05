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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Broker服务器中生产者管理对象
public class ProducerManager {
	
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    // 锁超时时间
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    
    // 当超过120s, 客户端未与服务端进行交互时
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    
    private final Lock groupChannelLock = new ReentrantLock();
    // 生产者一对一String /* group name */, HashMap<Channel, ClientChannelInfo>Map关系
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
        new HashMap<String, HashMap<Channel, ClientChannelInfo>>();

    public ProducerManager() {
    	
    }

    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
        	// 尝试获取锁groupChannelLock, 超时时间120ms
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                	// 将groupChannelTable存入Map对象中
                    newGroupChannelTable.putAll(groupChannelTable);
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return newGroupChannelTable;
    }

    // 将生产组中的无效生产者删除掉
    public void scanNotActiveChannel() {
        try {
        	// 尝试获取锁groupChannelLock, 超时时间120ms
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                        .entrySet()) {
                    	// 生产组名称
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();

                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            // 判断长时间未响应数据(120s)
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                it.remove();
                                log.warn(
                                    "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                    RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                // 关闭Channel对象
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    // 将指定Channel对象从生产组中删除,关闭Channel对象
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
            	// 尝试获取锁groupChannelLock, 超时时间120ms
                if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                            final String group = entry.getKey();
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                                entry.getValue();
                            // 删除channel所指向的value对象
                            final ClientChannelInfo clientChannelInfo =
                                clientChannelInfoTable.remove(channel);
                            if (clientChannelInfo != null) {
                            	// 日志记录Close事件触发,在remoting模块中触发事件
                                log.info(
                                    "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                    clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    } finally {
                    	// 释放锁资源
                        this.groupChannelLock.unlock();
                    }
                } else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    // 向Broker服务器中注册生产组对象
    // 但是为什么不去closeChannel对象呢?
    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            ClientChannelInfo clientChannelInfoFound = null;
            
            // 尝试获取锁groupChannelLock, 超时时间120ms
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                	// 获取group下的生产者集合对象
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null == channelTable) {
                        channelTable = new HashMap<>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    if (null == clientChannelInfoFound) {
                    	// 将生产者对象存入map对象中
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        log.info("new producer connected, group: {} channel: {}", group,
                            clientChannelInfo.toString());
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }

                if (clientChannelInfoFound != null) {
                	// 设置更新时间
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    // 从map对象中将channel key-value键值对删除
    // 但是为什么不去closeChannel对象呢?
    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
        	// 尝试获取锁groupChannelLock, 超时时间120ms
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {
                        // 从map中将生产者对象进行删除
                    	ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                clientChannelInfo.toString());
                        }

                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                } finally {
                	// 释放锁资源
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }
}
