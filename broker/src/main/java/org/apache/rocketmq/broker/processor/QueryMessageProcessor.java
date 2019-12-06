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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.pagecache.OneMessageTransfer;
import org.apache.rocketmq.broker.pagecache.QueryMessageTransfer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // Broker服务控制器对象
    private final BrokerController brokerController;

    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        
    	switch (request.getCode()) {
            case RequestCode.QUERY_MESSAGE:
            	// 使用IndexFile对象查询消息,并返回结果对象
                return this.queryMessage(ctx, request);
            case RequestCode.VIEW_MESSAGE_BY_ID:
            	// 向Broker服务器查询一个消息
                return this.viewMessageById(ctx, request);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
    	
    	// 获取响应报文数据
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
        final QueryMessageResponseHeader responseHeader =
            (QueryMessageResponseHeader) response.readCustomHeader();
        
        // 获取查询消息请求报文数据
        final QueryMessageRequestHeader requestHeader =
            (QueryMessageRequestHeader) request
                .decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        // 从Map中获取_UNIQUE_KEY_QUERY属性值
        String isUniqueKey = request.getExtFields().get(MixAll.UNIQUE_MSG_QUERY_FLAG);
        
        // true : 消息唯一Key == "true"
        if (isUniqueKey != null && isUniqueKey.equals("true")) {
        	// default = 32;
            requestHeader.setMaxNum(this.brokerController.getMessageStoreConfig().getDefaultQueryMaxNum());
        }

        // 使用IndexFile从commitLog中去查询指定数目的消息数据报文
        final QueryMessageResult queryMessageResult =
            this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
                requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
                requestHeader.getEndTimestamp());
        assert queryMessageResult != null;
        
        // 设置IndexService最新最大物理偏移量(lastIndexFile);
        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        
        // 设置IndexService最新更新时间戳(lastIndexFile);
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());

        if (queryMessageResult.getBufferTotalSize() > 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
            	// 将消息封装成FileRegion对象
                FileRegion fileRegion =
                    new QueryMessageTransfer(response.encodeHeader(queryMessageResult
                        .getBufferTotalSize()), queryMessageResult);
                
                // 写入Channel对象
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        queryMessageResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer query message by page cache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                queryMessageResult.release();
            }

            return null;
        }

        // 设置为消息找不到状态
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("can not find message, maybe time range not correct");
        return response;
    }

    public RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        
        // 获取网络请求报文数据ViewMessageRequestHeader对象
        final ViewMessageRequestHeader requestHeader =
            (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        // 获取消息ByteBuffer数据
        final SelectMappedBufferResult selectMappedBufferResult =
            this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());
        
        if (selectMappedBufferResult != null) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
            	// 将其封装成FileRegion对象
                FileRegion fileRegion =
                    new OneMessageTransfer(response.encodeHeader(selectMappedBufferResult.getSize()),
                        selectMappedBufferResult);
                
                // 将FileRegion对象写入Channel对象中去
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        selectMappedBufferResult.release();
                        if (!future.isSuccess()) {
                            log.error("Transfer one message from page cache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                selectMappedBufferResult.release();
            }

            return null;
        } else {
        	// 设置为系统错误
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("can not find message by the offset, " + requestHeader.getOffset());
        }

        return response;
    }
}
