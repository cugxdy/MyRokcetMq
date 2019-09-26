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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// 它作用RocketMq主从服务对象(集群主从同步)
public class HAService {
	
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 主从连接计数器(HAConnection计数器)
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    // 主从连接List对象 
    private final List<HAConnection> connectionList = new LinkedList<>();

    // 网络中accept服务,即接收客户端的连接请求
    private final AcceptSocketService acceptSocketService;

    // RocketMq数据存储对象
    private final DefaultMessageStore defaultMessageStore;

    // 线程阻塞与唤醒对象
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    
    // 推送至从服务器最大物理偏移量(该由从服务器发送更新数据)
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    // 用于对线程异步操作结果通知
    private final GroupTransferService groupTransferService;

    // 客户端对象
    private final HAClient haClient;

    // 创建HAService对象
    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        
    	this.defaultMessageStore = defaultMessageStore;
        // 创建accept服务(接收网络对端的连接对象)
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    // 更新master地址(主服务器)
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    // 存入GroupCommitRequest对象
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    // 判断当前写入数据与同步至从服务器偏移量 < 256M(主服务器单次向从服务器最大发送数据)
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    // 唤醒某些线程对象
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
            	// 其实就是唤醒GroupTransferService服务,去检查是否主从同步完成,通知业务线程
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    // 获取HAConnection连接计数
    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    // 开启主从集群服务
    public void start() throws Exception {
    	// 创建ServerSocketChannel对象,接收客户端的连接请求
        this.acceptSocketService.beginAccept(); 
        
        this.acceptSocketService.start();
        this.groupTransferService.start();
        // 启动客户端
        this.haClient.start();
    }

    // 将HAConnection对象添加进connectionList对象中
    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    // 从connectionList中删除HAConnection对象
    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    // shutdown服务
    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    // 关闭与其它服务器的连接对象
    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    // 获取DefaultMessageStore对象
    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    // 获取从服务器已接收偏移量
    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    // 监听客户端的连接请求,并创建HAConnection对象
    class AcceptSocketService extends ServiceThread {
    	// 监听连接的地址
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        
        // reactor模式轮询器
        private Selector selector;

        // 本地服务监听地址 (default = 10912)
        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        // 设置ServerSokcetChannel对象,并绑定监听端口
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override // 关闭Accept服务
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try { 
            	// 关闭ServerSocketChannel对象
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override// 启动accept服务
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                	// 阻塞线程
                    this.selector.select(1000);
                    // 实际上selected.size() <= 1
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                        	// 判断网络操作位是否为OP_ACCEPT标志(只允许OP_ACCEPT)
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                            	// 创建Tcp连接
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                	// 日志记录主从服务连接连接
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                    	// 创建HAConnection对象
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        // 将连接存入connectionList对象中
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        // 清理SelectKey集合对象
                        selected.clear();
                    }
                } catch (Exception e) {
                	// 异常发生
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            // 记录服务处于shutdown状态下
            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override // accept服务名称
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     */
    // 它是用于处理主从服务,主服务器向从服务器发送数据(commitLog文件集群一致性)
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        // 将request放入requestsWrite对象中
        // 将向从服务器写入数据(即将主服务器中putMessage提交成功的消息写入到从服务器中)
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        // 唤醒一个线程
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        // 将requestsWrite与requestsRead引用地址进行交换
        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                    	// 判断主从同步是否成功()
                    	// 计算理由为: push2SlaveMaxOffset < maxReadOffset(CommitLog) : 表明主服务器存在数据未同步至从服务器
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        
                        // maxWaitTime = 5s
                        for (int i = 0; !transferOK && i < 5; i++) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                        	// 同步主服务器超时
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // 唤醒等待主从同步成功的线程
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    // 等待主从同步完成
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override // 将两者引用进行互换
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override // 获取GroupTransferService服务名称
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    // 主从集群(客户端)
    class HAClient extends ServiceThread {
    	// MaxBufferSize = 4M;
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        
        // 主服务器网络地址
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        
        // 向主服务器发送当前已收到数据最大物理偏移量(ByteBuffer作为发送单元)
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        
        // SocketChannel对象
        private SocketChannel socketChannel;
        
        // reactor模式轮询器
        private Selector selector;
        
        // 最近从socketchannel读时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        // 记录向主服务发送的commitLog对象中的最大偏移量
        private long currentReportedOffset = 0;
        
        // 它记录着byteBufferRead已处理过的索引
        private int dispatchPostion = 0;
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        // 创建HAClient对象,并使用selector轮询器
        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        
        // 更新主服务器地址
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        // 判断是否距离上次写已有5s时间
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            // 判断是否需要发送心跳消息
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        // 将最大偏移量传入网络另一端(将已收到最大物理偏移量发送给主服务器)
        private boolean reportSlaveMaxOffset(final long maxOffset) {
        	// 将Long型数据写入到ByteBuffer对象中
        	this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // maxWriteTimes = 3;
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                	// 将ByteBuffer对象写入SocketChannel对象中
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        // 它byteBufferRead中dispatchPostion位置的数据移项向最前面去(0)
        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPostion;
            
            // 将byteBufferRead中[dispatchPostion-~]数据写入
            // byteBufferBackup以0为开始位置的ByteBuffer对象中
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPostion);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            // 将byteBufferBackup与byteBufferRead交换引用指向
            this.swapByteBuffer();

            // 将byteBufferRead清空,并重置处理索引
            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPostion = 0;
        }

        // 将byteBufferRead与byteBufferBackup的引用指向交换 
        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        // 从SocketChannel中读入数据
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            // 在写模式下,判断是否存在空间存储数据
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                    	// 记录写入时间戳
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        readSizeZeroTimes = 0;
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    // 当超过三次时,未读入数据,即退出循环
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                    	// SocketChannel未读入数据
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                	// 异常发生(从主服务器中读取数据出现异常)
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
            	
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                if (diff >= msgHeaderSize) {
                	
                	// 获取主服务器物理偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    // 获取Body大小
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    // 获取当前服务器中的commitLog对象当前最大物理偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) { // 判断是否为第一次向主服务器发送数据
                    	// 判断主从服务器数据是否一致
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    // 当为true时,即是主服务器向从服务器发送消息数据(commitLog)
                    if (diff >= (msgHeaderSize + bodySize)) {
                    	// 获得body数据(其实就是主服务器发送过来的消息内容)
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        // 设置position属性
                        this.byteBufferRead.position(readSocketPos);
                        this.dispatchPostion += msgHeaderSize + bodySize;

                        // 将当前系统中的最大偏移量发送给对端计算机
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        // 继续下一次的数据分析工作
                        continue;
                    }
                }

                // hasRemaining:当byteBufferRead写满时为false
                if (!this.byteBufferRead.hasRemaining()) {
                	// 当byteBufferRead数据写满时
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        // 向主服务器发送当前从服务器的commitLog最大索引号(即自己收到的数据)
        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            // 获取最大物理写偏移量
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                
                // 将commitLog中的最大物理偏移量传给网络对端
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                	// 关闭TCP通道
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        // 向远程服务器发起远程TCP连接请求
        private boolean connectMaster() throws IOException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                	// 获取远程TCP连接地址
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                    	
                    	// 获取SocketChannel对象
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                
                // 记录当前服务器最大物理偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                
                // 最近从socketchannel读时间戳
                this.lastWriteTimestamp = System.currentTimeMillis();
            }
            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                	// 获取在Selector上的SelectionKey对象,并撤销它
                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    // 关闭TCP通道
                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                // 将Client属性清空
                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                // 设置byteBufferBackup与byteBufferRead的属性值
                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            // 服务未处于shutdown状态中
            while (!this.isStopped()) {
                try {
                	// 发起TCP连接请求
                    if (this.connectMaster()) {

                    	// 发送心跳包
                        if (this.isTimeToReportOffset()) {
                        	// 向远程服务器使用Tcp协议发送CommitLog对象中最大物理偏移量数据
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 阻塞select
                        this.selector.select(1000);

                        // 处理read事件
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                        	// 关闭TCP连接
                            this.closeMaster();
                        }

                        // 发送最大物理偏移量
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        
                        // 主服务器长时间未向从服务器发送数据时,关闭TCP连接
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                        	// 长久未响应即关闭TCP连接
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                    	// 阻塞指定时间(5s)
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            // 记录服务shutdown
            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        
        @Override// 当前服务名称
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
