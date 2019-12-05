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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 它是主从同步,RocketMq集群配置时
public class SlaveSynchronize {
	
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    private final BrokerController brokerController;
    
    // 当服务器为从服务器时,配置的master主服务器地址
    private volatile String masterAddr = null;

    // 创建SlaveSynchronize对象
    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    // 从服务器向主服务器发送同步数据请求
    public void syncAll() {
        this.syncTopicConfig(); 
        this.syncConsumerOffset();
        this.syncDelayOffset();
        this.syncSubscriptionGroupConfig();
    }

    // 从主服务器中更新当前服务器的TopicConfig配置信息
    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                TopicConfigSerializeWrapper topicWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                
                // 使用主服务器获取的TopicConfigManager对象刷新本服务器的TopicConfigManager对象
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(topicWrapper.getDataVersion())) {

                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(topicWrapper.getTopicConfigTable());
                    this.brokerController.getTopicConfigManager().persist();

                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    // 从主服务器中更新当前服务器的ConsumerOffset配置信息
    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
            	// 向主服务器发出同步请求(SYNC)
                ConsumerOffsetSerializeWrapper offsetWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                
                // 更新当前服务器中consumerOffset配置信息
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(offsetWrapper.getOffsetTable());
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    // 向主服务器更新当前服务器中的Delay配置信息
    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
            	
                String delayOffset =
                    this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {
                	
                	// 获取delayOffset配置文件,config/delayOffset.json
                    String fileName =
                        StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                            .getMessageStoreConfig().getStorePathRootDir());
                    
                    try {
                    	// 将delayOffset字符串写入到文件中去
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    // 向主服务器更新当前服务器中的subscriptionGroup配置对象
    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
            	// 同步向主服务器发送同步请求,获取subscriptionGroup配置对象
                SubscriptionGroupWrapper subscriptionWrapper =
                    this.brokerController.getBrokerOuterAPI()
                        .getAllSubscriptionGroupConfig(masterAddrBak);

                // 判断主从服务器数据的版本号是否一致
                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                    .equals(subscriptionWrapper.getDataVersion())) {
                	
                    SubscriptionGroupManager subscriptionGroupManager =
                        this.brokerController.getSubscriptionGroupManager();
                    
                    // 更新数据版本号
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                        subscriptionWrapper.getDataVersion());
                    
                    // 更新当前服务器的SubscriptionGroup配置对象
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                        subscriptionWrapper.getSubscriptionGroupTable());
                    
                    // 将当前SubscriptionGroupManager对象写入config/subscriptionGroup.json文件中
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}
