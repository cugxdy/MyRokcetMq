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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.store.config.BrokerRole.SLAVE;

public class DefaultMessageStore implements MessageStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 消息存储配置对象 
    private final MessageStoreConfig messageStoreConfig;
    
    // CommitLog对象
    private final CommitLog commitLog;

    // topic-queueId的消费队列
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    // ConsumeQueue刷盘服务线程
    private final FlushConsumeQueueService flushConsumeQueueService;

    // 清理CommitLog队列
    private final CleanCommitLogService cleanCommitLogService;

    // 清理消费队列服务(ConsumeQueue)
    private final CleanConsumeQueueService cleanConsumeQueueService;

    // 索引服务
    private final IndexService indexService;

    // MappedFile分配线程,RocketMQ使用内存映射处理commitlog,consumeQueue文件
    private final AllocateMappedFileService allocateMappedFileService;

    // 重试存储消息服务现场
    private final ReputMessageService reputMessageService;

    // 主从同步实现服务
    private final HAService haService;

    // 定时任务调度器，执行定时任务，主要是处理定时任务。
    private final ScheduleMessageService scheduleMessageService;

    // 存储统计服务
    private final StoreStatsService storeStatsService;

    // 堆外内存池
    private final TransientStorePool transientStorePool;

    // 存储服务状态
    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    // 定时任务线程池
    private final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    
    // Broker统计服务
    private final BrokerStatsManager brokerStatsManager;
    
    private final MessageArrivingListener messageArrivingListener;
    
    // Broker配置选项
    private final BrokerConfig brokerConfig;

    // 是否处于shutdown状态下
    private volatile boolean shutdown = true;

    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    // 转发comitlog日志,主要是从commitlog转发到consumeQueue、commitlog、index。
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    // file锁
    private FileLock lock;

    // 正常模式下shutdown
    boolean shutDownNormal = false;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        
    	// 配置项
    	this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        
        // store统计服务
        this.storeStatsService = new StoreStatsService();
        
        // 创建IndexService与HAservice服务
        this.indexService = new IndexService(this);
        this.haService = new HAService(this);

        this.reputMessageService = new ReputMessageService();

        // 处理定时任务
        this.scheduleMessageService = new ScheduleMessageService(this);

        // 堆外线程池
        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        // 启动创建MappedFile文件服务
        this.allocateMappedFileService.start();

        // 启动index服务
        this.indexService.start();

        this.dispatcherList = new LinkedList<>();
        
        // 添加ConsumeQueue与IndexFile建造类
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        
        // C:\Users\Administrator\store\lock
        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        // 确保父目录存在
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }

    // 从ConsumeQueue中删除物理偏移量大于phyOffset的数据
    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
            	// 在ConsumeQueue中删除OffsetPy(CommitLog物理偏移量)大于 phyOffset的数据
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
        	// 判断 ${ROCKET_HOME}/storepath/abort 文件是否存在，如果文件存在，则返回true,否则返回false
        	// 判断是否为正常shutdown
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            if (null != scheduleMessageService) {
            	// 延迟消息服务启动
                result = result && this.scheduleMessageService.load();
            }

            // load Commit Log
            // commitlog文件加载
            result = result && this.commitLog.load();

            // load Consume Queue
            // 加载consumerqueue文件
            result = result && this.loadConsumeQueue();

            if (result) {
            	// 文件存储检测点
                this.storeCheckpoint =
                    new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                // 索引文件加载
                this.indexService.load(lastExitOK);

                // 文件检测恢复
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
        	// shutdown AllocateMappedFileService服务
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * @throws Exception
     */
    // lockFile = C:\Users\Administrator\store\lock
    public void start() throws Exception {
    	// 尝试上锁,上锁成功,1s自动解锁
        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
        	// 抛出RuntimeException异常.
            throw new RuntimeException("Lock failed,MQ already started");
        }

        // 写入lock字符串
        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true); // 刷新

        // 启动flushConsumeQueue服务、commitLog服务、store统计服务
        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        // 当为主服务器时,启动scheduleMessageService服务
        if (this.scheduleMessageService != null && SLAVE != messageStoreConfig.getBrokerRole()) {
            this.scheduleMessageService.start();
        }

        // 判断是否开启复制选项
        if (this.getMessageStoreConfig().isDuplicationEnable()) {
            this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
        } else {
            this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
        }
        
        // reputMessageService服务启动
        this.reputMessageService.start();

        // 启动主从服务
        this.haService.start();

        // 创建临时文件
        this.createTempFile();
        // 添加定时任务(对consumeQueue、indexFile、commitLog对象进行定时检查的周期任务)
        this.addScheduleTask();
        this.shutdown = false;
    }

    // shutdown store服务
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {
            	// 线程休息3s
                Thread.sleep(1000 * 3);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }

            this.haService.shutdown();

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            // 判断是否commitLog文件均生成构建indexFile、consumeQueue成功
            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
            	// 正常退出前,删除abort文件
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        // shutdown 堆外内存池
        this.transientStorePool.destroy();

        // 是否锁对象
        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
    	// 删除consumeQueue下的所有mappedFile文件
        this.destroyLogics();
        // 删除commitLog下的所有mappedFile文件
        this.commitLog.destroy();
        // 删除indexFile下的所有mappedFile文件
        this.indexService.destroy();
        // 删除abort文件
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        // 删除checkpoint文件
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    // 删除consumeQueue下的所有mappedFile文件
    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // 判断是否处于shutdown状态下
    	if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

    	// 如果为从服务器,禁止写操作
        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // 判断当前运行状态是否具有写能力
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        // Topic大于127时
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        // 检测操作系统页写入是否忙
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        // 获取当前时间
        long beginTime = this.getSystemClock().now();
        // 将日志写入CommitLog文件，具体实现类CommitLog
        PutMessageResult result = this.commitLog.putMessage(msg);

        // 当大于500ms时,记录存储消息所花费时间
        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
        }
        
        // 记录相关统计信息  // 递增消费时间计数器
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        // 记录写commitlog失败次数
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
    	// 判断是否shutdown中
        if (this.shutdown) {
        	// shutdown SERVICE_NOT_AVAILABLE
            log.warn("DefaultMessageStore has shutdown, so putMessages is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // 当为从服务器时, SERVICE_NOT_AVAILABLE
        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is in slave mode, so putMessages is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // 当前服务器不具备写能力 SERVICE_NOT_AVAILABLE
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is not writable, so putMessages is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        // 消息Topic 长度 超长 MESSAGE_ILLEGAL
        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("PutMessages topic length too long " + messageExtBatch.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 消息properties 长度 超长 MESSAGE_ILLEGAL
        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 检测操作系统页写入是否忙
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        long beginTime = this.getSystemClock().now();
        // 将消息存储在commitLog文件对象中
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);

        long eclipseTime = this.getSystemClock().now() - beginTime;
        // 当大于500ms时,记录存储消息所花费时间
        if (eclipseTime > 500) {
            log.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
        }
        
        // 递增消费时间计数器
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        // putMessage 失败时
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    @Override // 判断OS繁忙
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        // 判断锁持有时间是否大于 > default = 1000ms
        return diff < 10000000
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override // 获取距离上锁已过多少时间
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    // 获取时钟对象
    public SystemClock getSystemClock() {
        return systemClock;
    }

    // 获取commitLog对象
    public CommitLog getCommitLog() {
        return commitLog;
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums,
        final MessageFilter messageFilter) {
    	
        if (this.shutdown) {
        	// 该服务器处于shutdown中
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
        	// 该服务器不具备读能力
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        // 记录读开始时间戳
        long beginTime = this.getSystemClock().now();

        // 取消息状态
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        // 获取commitLog对象最大物理偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        // 获取topic-queueId下的consumeQueue对象
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
        	// 获取最小索引号(单位:CQ_STORE_UNIT_SIZE)
        	// 获取最大索引号(单位:CQ_STORE_UNIT_SIZE)
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
            	// 不存在消息
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
            	// 当offset < 最小索引时 
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
            	// 当offset == 最大索引时 
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
            	// 当offset > 最大索引时 
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                    	// 记录目前状态
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        
                        // default = true
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 获取msg所在commitLog中物理偏移量
                        	long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        	// 获取消息大小
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            // 获取tagsCode
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            // 判断是否处于磁盘上(实际上没什么作用)
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            // 判断消息获取结果已满()
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                isInDisk)) {
                                break;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;
                            // 当tagsCode < -2147483648时 = true
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                	// 获取tagsCode值
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                	// 日志记录出错信息
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                        tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            // 判断是否需要过滤掉本次tagsCode所指向的message
                            if (messageFilter != null
                                && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }
                            
                            // 从commitLog中获取数据(offsetPy为startOffset, sizePy为长度)
                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }
                                
                                // 获取从offset算起下一个mappedFile的fileFromOffset
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            // 对message进行过滤 :(default = isMatchedByCommitLog(true))
                            if (messageFilter != null
                                && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            // 递增获取成功计数器
                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                            getResult.addMessage(selectResult);
                            // 记录消息获取状态
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        if (diskFallRecorded) {
                        	// 计算差值 = commitLog文件对象最大物理偏移量 - 本次查询消息中最大物理偏移量
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }
                        
                        // 下一次getMessage建议开始获取消息索引开始位置
                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                            * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        // 建议从从服务器获取message
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {
                    	// 释放SelectMappedByteBuffer对象
                        bufferConsumeQueue.release();
                    }
                } else {
                    // 该索引获取不到message(consumeQueue)
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                        + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
        	// 该topic-queueId不存在consumeQueue对象
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long eclipseTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);

        // 设置GetMessageResult属性值
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    // 获取在consumeQueue最大物理偏移量(单位:CQ_STORE_UNIT_SIZE)
    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    // 获取在consumeQueue最小物理偏移量(单位:CQ_STORE_UNIT_SIZE)
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override // 获取在consumeQueue对象中以consumeQueueOffset为开始位置的存储单元的commitLog物理偏移量
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
        	// 获取以consumeQueueOffset开始的数据
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    // 返回commitLog偏移量
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    // 根据时间戳获取message在consumeQueue中的索引(单位:CQ_STORE_UNIT_SIZE)
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
        	// 获取timestamp所在的message在consumeQueue中的存储单元索引
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    // 根据commitLogOffset索引从commitLog获取MessageExt对象
    public MessageExt lookMessageByOffset(long commitLogOffset) {
    	// 这个主要是为了获取message的总长度
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                // 从CommitLog中获取MessageExt对象
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override // 获取CommitLog下commitLogOffset索引处的消息(SelectMappedBufferResult对象)
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override // 获取CommitLog下commitLogOffset索引处的消息(SelectMappedBufferResult对象)
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    // 获取运行统计数据
    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
    	// 获取运行统计信息
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            // 获取commitLog所属文件夹磁盘使用率
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            // 记录commitLog磁盘使用比率
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            // 获取consumeQueue所属文件夹磁盘使用比率
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            // 记录consumeQueue磁盘使用比率
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
        	// 记录定时服务中的信息
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        // 记录commitLog中最小物理偏移量
        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        // 记录commitLog中最大物理偏移量
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override // 获取commitLog中最大物理偏移量
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override // 获取commitLog中最小物理偏移量
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override // 获取最早message的存储时间
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
        	// 获取最早message的consumeQueue偏移量
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            // 获取message的存储时间
            return getStoreTime(result);
        }

        return -1;
    }

    // 获取message的存储时间
    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
            	// commitLog偏移量
                final long phyOffset = result.getByteBuffer().getLong();
                // message消息大小
                final int size = result.getByteBuffer().getInt();
                // 获取消息message存储时间
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override // 从commitLog中获取最早消息的存储时间
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override// 获取指定consumeQueueOffset索引处消息的存储时间
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            // 获取message的存储时间
            return getStoreTime(result);
        }

        return -1;
    }

    @Override // 获取在队列中的消息总数
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
        	// 计算公式 = maxOffsetInQueue - minOffsetInQueue
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override // 从commitLog中获取offset为开始位置之后的mappedFile数据
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override // 将字节数组appendmappedFile对象(从服务器从主服务器接收数据向commitLog中写入数据)
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
        	// shutdown中
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        // 将data字节数组append 到 mappedFile对象中
        boolean result = this.commitLog.appendData(startOffset, data);
        if (result) {
        	// 唤醒RePutMessage服务,构建consumeQueue、IndexFile对象
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override // 设置主动清理mappedFile对象(default = 20)
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    @Override // 查询消息(使用IndexFile对象来查询消息)
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
        	// 获取指定key值Message所对应的CommitLog物理偏移量
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            // 对long型数据排序
            Collections.sort(queryOffsetResult.getPhyOffsets());

            // 设置IndexFile中最大物理偏移量与indexFile最大更新时间戳
            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                // 获取commitLog物理偏移量
            	long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    // 依据offset获取消息MessageExt对象
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                    	// 记录消息存储时间戳
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                    	// 获取该消息在offset位置上的selectMappedBufferResult对象
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            // 将所获取的消息ByteBuffer对象存入queryMessageResult对象中
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        // 返回查询消息结果对象
        return queryMessageResult;
    }

    @Override // 更新主服务器地址
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override // 获取当前系统时间
    public long now() {
        return this.systemClock.now();
    }

    @Override // 删除指定不在topics中的topic所对应的consumeQueue对象(SCHEDULE_TOPIC_XXXX)
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            
            // 当topic不存在于topics中时,将topic下consumeQueue对象均删除
            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                // 遍历ConsumeQueue对象
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy(); // 销毁ConsumeQueue对象
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                        cq.getTopic(),
                        cq.getQueueId()
                    );

                    // 从commitLog中删除topic-queueId键值对
                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                // 从consumeQueueTable中删除topic所对应的value值
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    // 销毁过期consumeQueue:consumeQueue中所存储最大commitLog偏移量 < 实际上commitLog中的最小物理偏移量
    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    // 获取consumeQueue中最后存储单元所指向的commitLog消息索引(offset + size)
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) { // consumeQueue刚刚创建
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                            nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId(),
                            nextQT.getValue().getMaxPhysicOffset(),
                            nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        // 当该maxCLOffsetInConsumeQueue < commitLog中最小物理偏移量
                    	log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                            topic,
                            nextQT.getKey(),
                            minCommitLogOffset,
                            maxCLOffsetInConsumeQueue);

                    	// 从commitLog中删除该consumeQueue所指向的索引号
                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId());

                        // 销毁该consumeQueue对象
                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                // 判断ConcurrentMap<Integer, ConsumeQueue>是否为空
                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    // 获取topic-queueId下的consumeQueue队列存储消息的Id map对象
    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
        SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) { // 处于shutdown中,返回空Map对象
            return messageIds;
        }

        // 获取topic-queueId下的consumeQueue对象
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
        	// 获取最小偏移量
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            // 获取最大偏移量
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            // 获取consumeQueue所有存储单元所指向的消息Offset的messaegId
            // 格式: msgId - minOffset;
            //      msgId1 - minOffset + 1;
            //      msgId2 - minOffset + 2;
            while (nextOffset < maxOffset) {
            	// 获取该consumeQueue中nextOffset所在mappedFile对象所有ByteBuffer数据
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 获取该存储单元指向的物理索引
                        	long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        	// 消息ID ByteBuffer对象
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                            String msgId =
                                MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {
                    	// 释放ByteBuffer对象
                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        // 获取该topic-queueId下的consumeQueue对象
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
        	// 获取该consumeOffset下所属mappedFile的数据
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    // 判断热putMessage服务是否将commitLog创建IndexFile、consumeQueue对象
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override // commitLog刷新服务
    public long flush() {
        return this.commitLog.flush();
    }

    @Override // commitLog重置索引
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override // 获取commitLog中确认索引
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override // 设置commitLog中确认索引
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    // 从CommitLog中获取消息(MessageExt对象)
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
    	// 获取commitLogOffset索引处的消息ByteBuffer对象
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
            	// 将消息解码成MessageExt对象
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
    	// 获取topic下的<Integer, consumeQueue> map对象
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
        	// 创建ConcurrentHashMap对象
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        // 获取consumeQueue对象
        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
        	// 创建consumeQueue对象
            ConsumeQueue newLogic = new ConsumeQueue(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                this);
            
            // 将consumeQueue对象存入map对象中
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    // !SLAVE  = return: newOffset;
    // SLAVE   = return: oldOffset(offsetCheckInSlave=false);
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        // 主服务器|offsetCheckInSlave = true
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    // 判断所取偏移量是否处于整个mappedFile内存的前40%
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

    	// 第一次循环时,直接就返回
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        // getResult中消息总数  > 最大允许消息总数
        if (maxMsgNums <= messageTotal) {
            return true;
        }

        if (isInDisk) {
        	// (getResult)判断是否当前获得消息已达到 (default = 64kb)
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }
            
            // (getResult)判断是否当前获得消息总数已达(default = 8)
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        } else {
        	// (getResult)判断是否当前获得消息已达到 (default = 64kb)
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            // (getResult)判断是否当前获得消息总数已达(default = 8)
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    // 删除特定fileName文件
    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    // 创建临时文件(C:\Users\Administrator\store\abort)
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        // 创建abort文件
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    // 向定时线程池中添加线程池定时|周期任务
    private void addScheduleTask() {

    	// 周期任务为10000ms = 10s
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        // 周期任务为10s
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
            	// 检查mappedFile文件是否符合指定大小
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        // 周期任务为1s
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
            	// 判断是否对lock超时日志记录
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            // 获取commitLog获取锁时间
                        	long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            // 判断锁是否长久未释放
                        	if (lockTime > 1000 && lockTime < 10000000) {
                        		
                        		// 获取线程堆栈
                                String stack = UtilAll.jstack();
                                // 获取文件名
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                    + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                // 将堆栈信息存入日志文件中
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    // 将cleanCommitLogService、cleanConsumeQueueService服务运行一次
    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }

    // 检查MappedFile文件大小是否符合指定大小
    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            // 对ConsumeQueue对象检查MappedFile文件大小是否符合指定大小
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    // 判断C:\Users\Administrator\store\abort 文件是否存在
    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    // 加载ConsumeQueue对象
    private boolean loadConsumeQueue() {
    	// 文件路径名为: C:\Users\Administrator\store\consumequeue
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {
        	// 遍历consumequeue目录下的文件对象
            for (File fileTopic : fileTopicList) {
            	// 获取文件名(即topic名称)
                String topic = fileTopic.getName();

                // 获取topic目录下的queueId文件列表
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                	// 遍历queueId文件列表
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                        	// 获取队列Id(queueId)
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;  // 非数字字符串
                        }
                        
                        // 创建ConsumeQueue对象
                        ConsumeQueue logic = new ConsumeQueue(
                            topic,
                            queueId,
                            StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                            this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                            this);
                        
                        // 将consumeQueue对象添加至consumeQueueTable列表中
                        this.putConsumeQueue(topic, queueId, logic);
                        
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    // 对ConsumeQueue、commitLog、
    private void recover(final boolean lastExitOK) {
        this.recoverConsumeQueue(); // 恢复消息队列

        // 如果是正常退出，则按照正常修复；如果是异常退出，则走异常修复逻辑
        if (lastExitOK) {
            this.commitLog.recoverNormally();
        } else {
            this.commitLog.recoverAbnormally();
        }

        // 修复主题队列
        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    // 获取堆外内存池
    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    // 将ConsumeQueue存入consumeQueueTable中
    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) { // 为null时,创建Map对象存入consumeQueueTable中
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    // 对ConsumeQueue中存储单元校验,并进行重置commit、flush索引
    private void recoverConsumeQueue() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
            }
        }
    }

    // 对commitLog中topicQueueTable属性值进行设置
    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        // 获取commitLog中最小物理偏移量
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                // getMaxOffsetInQueue:获取最大索引(单位20字节)
                table.put(key, logic.getMaxOffsetInQueue());
     
                // 纠正consumeQeueue对象最小逻辑偏移量
                logic.correctMinOffset(minPhyOffset);
            }
        }

        // 存入CommitLog对象的topicQueueTable属性中
        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    // 获取StoreCheckpoint对象
    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    // 获取主从消息服务
    public HAService getHaService() {
        return haService;
    }

    // 获取定时消息服务
    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    // 获取系统运行flag
    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    // 构建IndexFile、ConsumeQueue对象中的数据
    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    // 判断剩余堆外内存余数
    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.remainBufferNumbs();
    }

    @Override // 堆外内存池是否可用
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override // 获取dispatcherList属性值
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override // 获取指定topic-queueId下的ConsumeQueue对象
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    // 创建定时任务(对文件进行解锁【JNI】)
    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS); // 6s后执行
    }

    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
        	// 判断消息类型
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
        	// 判断IndexFile对象对象允许使用
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                // 构建indexFile文件条目
            	DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
        
        // 磁盘使用警告线,需对文件进行清理
        private final double diskSpaceWarningLevelRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        // 磁盘使用强制清理比率值
        private final double diskSpaceCleanForciblyRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        
        private long lastRedeleteTimestamp = 0;

        // default = 20 主动去删除mappedFile计数器(无论是否在凌晨4点或者磁盘满溢)
        private volatile int manualDeleteFileSeveralTimes = 0;

        // 是否立即清除
        private volatile boolean cleanImmediately = false;

        // 设置manualDeleteFileSeveralTimes属性值
        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
            	// 执行commitLog下的mappedFile对象删除工作
                this.deleteExpiredFiles();
                
                // 
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteCount = 0;
            // default = 72ms;
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            // default = 100ms;
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            // default = 1000 * 120ms
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
            
            // default : 判断是否为凌晨4点
            boolean timeup = this.isTimeToDelete();
            
            // 判断磁盘空间是否充裕 : true => 需clean
            boolean spacefull = this.isSpaceToDelete();
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                // 判断是否需要立即清除
                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                    fileReservedTime,
                    timeup,
                    spacefull,
                    manualDeleteFileSeveralTimes,
                    cleanAtOnce);
                
                // default = 72 * 60 * 60 * 1000; 72小时
                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                    destroyMapedFileIntervalForcibly, cleanAtOnce);
                
                // deleteCount:删除mappedFile对象计数
                if (deleteCount > 0) {
                	
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        private void redeleteHangedFile() {
        	// default = 1000 * 120
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                
                // default = 1000 * 120
                int destroyMapedFileIntervalForcibly =
                    DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                
                // 尝试去删除第一个commitLog中的MappedFile对象
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                
                }
            }
        }

        // 获取CleanCommitLogService服务名称
        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        // default : 判断是否为凌晨4点
        private boolean isTimeToDelete() {
        	// default = 04
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            // default : 判断是否为凌晨4点
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        // 判断磁盘是否充裕
        private boolean isSpaceToDelete() {
        	// 获取磁盘使用比率(10 - 95) / 100.0
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            {
            	// 获取commitlog文件存储路径
                String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                
                // 获取磁盘使用比率
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                
                // 比率超过 > 0.90
                if (physicRatio > diskSpaceWarningLevelRatio) {
                	// 设置系统运行状态标识为DISK_FULL_BIT
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                	// 清除DISK_FULL_BIT标识,磁盘使用尚足
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                // 磁盘使用比率超过使用上限(DefaultMessageStore.diskMaxUsedSpaceRatio)
                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            {
            	// 获取consumeQueue对象存储路径
                String storePathLogics = StorePathConfigHelper
                    .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                
                // 获取磁盘使用比率
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                
                // 比率超过 > 0.90
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                	// 设置系统运行状态标识为DISK_FULL_BIT
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                // 比率超过 > 0.85
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                	
                	// 清除DISK_FULL_BIT标识,磁盘使用尚足
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                // 磁盘使用比率超过使用上限(DefaultMessageStore.diskMaxUsedSpaceRatio)
                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        // 获取执行deleteMappedFile次数
        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        // 设置执行deleteMappedFile次数
        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    class CleanConsumeQueueService {
    	// 记录commitLog中最小物理偏移量
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
            	// 删除小于commitLog中最小物理偏移量的ConsumeQueue、indexFile对象
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
        	// default = 100ms
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            // 获取commitLog对象最小物理偏移量
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                    	// 对consumeQueue中小于minOffset偏移量的mappedFile对象进行删除
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                            	// 线程阻塞指定时间 (default = 100ms)
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        // 获取CleanConsumeQueueService服务名称
        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
    	
    	// 最大重试次数
        private static final int RETRY_TIMES_OVER = 3;
        
        // 上一次刷新时间
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
        	// consumeQueue刷新页数: default = 2;
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            // default = 1000 * 60
            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                // 从storeCheckpoint中的logicsMsgTimestamp属性值
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        // 对每个topic-queueId下的consumeQueue执行flush操作
                    	result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }
            
            // 设置logicsMsgTimestamp,并执行刷新
            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        public void run() {
        	// FlushConsumeQueueService 服务启动
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                	// default = 1000;
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    // 阻塞指定间隔时间
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            // FlushConsumeQueueService 服务 shutdown
            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override // 获取FlushConsumeQueueService服务名称
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override // 获取join时间
        public long getJointime() {
            return 1000 * 60;
        }
    }

    class ReputMessageService extends ServiceThread {

        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                    DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        // 计算剩余多少数据进行数据判断
        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        // 判断是否属性reputFromOffset小于commitLog中最大物理偏移量
        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        private void doReput() {
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

            	// 当reputFromOffset大于commitLog中的confirmOffset属性
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                    && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 从commitLog中获取从reputFromOffset为开始位置的数据
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                    	// 获取result在commitLog中开始的位置
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                        	// 获取msg数据(4 + 4 + 4 + 4 + 4 + 8 + 
                        	// 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8 + 
                        	// bodylen + body + topicLen + topic + propLen + prop)
                            DispatchRequest dispatchRequest =
                                DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            
                            // 获取消息总长度
                            int size = dispatchRequest.getMsgSize();

                            // 消息available
                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                	// 将消息用于consumeQueue、IndexFile对象中
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // 当服务器为主服务器时
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                        && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                    	
                                    	// 触发MessageArrivingListener对象中的arriving方法
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                            dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                            dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                            dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                    }
                                    
                                    // 递增reputFromOffset索引
                                    this.reputFromOffset += size;
                                    readSize += size;
                                    // 当为从服务器时
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        // 递增Topic下的消息计数器
                                    	DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        // 递增Topic下的消息占用总字节数
                                    	DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                            .addAndGet(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) { // 已到达文件末尾
                                	// 获取下一个MappedFile对象的开始物理偏移量
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) { // 消息 not available

                                if (size > 0) {
                                	// 消息计数不正确
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    // 当为主服务器时
                                    if (DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]the master dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                            this.reputFromOffset);
                                        
                                        // 计算reputFromOffset
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                	// 已到达commitLog文件队列末尾
                    doNext = false;
                }
            }
        }

        @Override
        public void run() {
        	// ReputMessageService服务启动
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // ReputMessageService服务 shutdown
            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override // 获取ReputMessageService服务名称
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
