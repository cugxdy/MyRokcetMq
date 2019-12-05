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

package org.apache.rocketmq.broker.filtersrv;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterServerManager {

	// filterService最大空闲时间戳
    public static final long FILTER_SERVER_MAX_IDLE_TIME_MILLS = 30000;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    private final ConcurrentMap<Channel, FilterServerInfo> filterServerTable =
        new ConcurrentHashMap<Channel, FilterServerInfo>(16);
    
    private final BrokerController brokerController;

    // 定时服务线程池
    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FilterServerManagerScheduledThread"));

    // 创建FilterServerManager对象
    public FilterServerManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {

    	// 定时任务, 5s启动, 30s周期
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                	// 创建过滤服务器
                    FilterServerManager.this.createFilterServer();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 5, 1000 * 30, TimeUnit.MILLISECONDS);
    }

    // 创建filterServer服务
    public void createFilterServer() {
    	// 获取过滤服务器地址
        int more =
            this.brokerController.getBrokerConfig().getFilterServerNums() - this.filterServerTable.size();
        
        // 创建cmd中命令形式
        // start ...
        String cmd = this.buildStartCommand();
        
        for (int i = 0; i < more; i++) {
        	// 使用java Process类,创建进程对象
            FilterServerUtil.callShell(cmd, log);
        }
    }

    // sh /bin/startfsrv.sh -c configFile -n namesrvAddr; 
    private String buildStartCommand() {
        String config = "";
        // 获取配置的config文件
        if (BrokerStartup.configFile != null) {
            config = String.format("-c %s", BrokerStartup.configFile);
        }

        // 配置namesrv服务地址
        if (this.brokerController.getBrokerConfig().getNamesrvAddr() != null) {
            config += String.format(" -n %s", this.brokerController.getBrokerConfig().getNamesrvAddr());
        }

        if (RemotingUtil.isWindowsPlatform()) {
        	// window下,使用start mqfiltersrv.exe命令
            return String.format("start /b %s\\bin\\mqfiltersrv.exe %s",
                this.brokerController.getBrokerConfig().getRocketmqHome(),
                config);
        } else {
        	// linux平台下,使用sh startsrv.sh命令
            return String.format("sh %s/bin/startfsrv.sh %s",
                this.brokerController.getBrokerConfig().getRocketmqHome(),
                config);
        }
    }

    // shutdown定时服务
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    // 向filterServerTable中写入FilterServerInfo对象
    public void registerFilterServer(final Channel channel, final String filterServerAddr) {
        FilterServerInfo filterServerInfo = this.filterServerTable.get(channel);
        if (filterServerInfo != null) {
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        } else {
            filterServerInfo = new FilterServerInfo();
            filterServerInfo.setFilterServerAddr(filterServerAddr);
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
            this.filterServerTable.put(channel, filterServerInfo);
            log.info("Receive a New Filter Server<{}>", filterServerAddr);
        }
    }

    // 定时去删除无效Channel对象(即长时间未响应的Channel对象)
    public void scanNotActiveChannel() {

        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            long timestamp = next.getValue().getLastUpdateTimestamp();
            Channel channel = next.getKey();
            
            // 判断空闲时间是否大于30s, 在30s内Broker与filter服务器未存储信息交互时
            if ((System.currentTimeMillis() - timestamp) > FILTER_SERVER_MAX_IDLE_TIME_MILLS) {
                log.info("The Filter Server<{}> expired, remove it", next.getKey());
                it.remove();
                RemotingUtil.closeChannel(channel);
            }
        }
    }

    // 处理Channel Close事件
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
    	// 删除channel对象
        FilterServerInfo old = this.filterServerTable.remove(channel);
        if (old != null) {
            log.warn("The Filter Server<{}> connection<{}> closed, remove it", old.getFilterServerAddr(),
                remoteAddr);
        }
    }

    // 获取所有的过滤服务器地址List对象(String)
    public List<String> buildNewFilterServerList() {
        List<String> addr = new ArrayList<>();
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            addr.add(next.getValue().getFilterServerAddr());
        }
        return addr;
    }

    // 过滤服务器信息(filterServiceInfo)
    static class FilterServerInfo {
    	
    	// 过滤服务地址
        private String filterServerAddr;
        
        // 最新更新时间戳
        private long lastUpdateTimestamp;

        public String getFilterServerAddr() {
            return filterServerAddr;
        }

        public void setFilterServerAddr(String filterServerAddr) {
            this.filterServerAddr = filterServerAddr;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }
}
