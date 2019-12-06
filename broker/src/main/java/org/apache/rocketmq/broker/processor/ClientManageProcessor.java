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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;


import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientManageProcessor implements NettyRequestProcessor {
    
	private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    // Broker服务器控制器对象
    private final BrokerController brokerController;

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
            	// 处理心跳报文,管理客户端
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
            	// 注销客户端
                return this.unregisterClient(ctx, request);
            case RequestCode.CHECK_CLIENT_CONFIG:
            	// 检查客户端发送的SubscriptionData对象, 服务器是否接受
                return this.checkClientConfig(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    // 它是处理客户端发送至Broker服务器的心跳包数据
    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        
        // 获取请求报文数据(心跳包)
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        
        // 获取客户端Id所对应的ClientChannelInfo对象
        // 即相当于MQClientInstance对象, 它其中可以包含多个生产者与消费者
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            heartbeatData.getClientID(),
            request.getLanguage(),
            request.getVersion()
        );

        
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
        	
            SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                    data.getGroupName());
            
            boolean isNotifyConsumerIdsChangedEnable = true;
            if (null != subscriptionGroupConfig) {
            	// default = true; 
                isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                int topicSysFlag = 0;
                
                // 0000 0010
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                
                // 获取%RETRY% + consumerGroup
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                
                // 创建TopicConfig对象(即Broker服务器上重试主题topic)
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                    newTopic,
                    subscriptionGroupConfig.getRetryQueueNums(),
                    PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            // 将消费者对象注册至Broker服务器上的ConsumerManager对象
            boolean changed = this.brokerController.getConsumerManager().registerConsumer(
                data.getGroupName(),
                clientChannelInfo,
                data.getConsumeType(),
                data.getMessageModel(),
                data.getConsumeFromWhere(),
                data.getSubscriptionDataSet(),
                isNotifyConsumerIdsChangedEnable
            );

            // true : ConsumerGroupInfo == CURD
            if (changed) {
                log.info("registerConsumer info changed {} {}",
                    data.toString(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                );
            }
        }

        // 将生产者对象注册至Broker服务器上的ProducerManager对象
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(),
                clientChannelInfo);
        }
        
        // 设置为成功状态 
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    // 向当前Broker服务器注销客户端请求处理方法
    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
    	
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        
        final UnregisterClientRequestHeader requestHeader =
            (UnregisterClientRequestHeader) request
                .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        // 获取客户端Id所对应的ClientChannelInfo对象
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            requestHeader.getClientID(),
            request.getLanguage(),
            request.getVersion());
        
        
        {
        	// 向生产组中注销客户端对象
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        {
        	// 向消费 组中注销客户端对象
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                	// default : true
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        // 设置为成功标识
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    // 检查客户端发送的SubscriptionData对象, 服务器是否接受
    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 请求报文中body数据
        CheckClientRequestBody requestBody = CheckClientRequestBody.decode(request.getBody(),
            CheckClientRequestBody.class);

        if (requestBody != null && requestBody.getSubscriptionData() != null) {
        	
            SubscriptionData subscriptionData = requestBody.getSubscriptionData();

            // true : null | TAG
            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            // true : enablePropertyFilter = false
            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return response;
            }

            try {
            	// 解析expression字符串
                FilterFactory.INSTANCE.get(subscriptionData.getExpressionType()).compile(subscriptionData.getSubString());
            } catch (Exception e) {
                log.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}",
                    requestBody.getClientId(), requestBody.getGroup(), requestBody.getSubscriptionData(), e.getMessage());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark(e.getMessage());
                return response;
            }
        }

        // 设置成功状态
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
