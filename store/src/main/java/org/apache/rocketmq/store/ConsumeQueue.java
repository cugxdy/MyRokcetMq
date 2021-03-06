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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 磁盘存储单元大小
    public static final int CQ_STORE_UNIT_SIZE = 20;
    
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    // MappedFile对象集合
    private final MappedFileQueue mappedFileQueue;
    // 该ConsumeQueue所属Topic
    private final String topic;
    // 该ConsumeQueue所属队列Id
    private final int queueId;
    
    private final ByteBuffer byteBufferIndex;

    // C:\Users\Administrator\store\consumequeue
    private final String storePath;
    private final int mappedFileSize;
    
    // 当前ConsumeQueue对象中最大有效物理偏移量
    private long maxPhysicOffset = -1;
    
    // 最小逻辑偏移量
    private volatile long minLogicOffset = 0;
    
    // ConsumeQueue扩展属性
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final DefaultMessageStore defaultMessageStore) {
        
    	this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        // 该ConsumeQueue对象所对应的文件目录
        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        // 创建MappedFileQueue对象
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        // 20字节长度ByteBuffer对象
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            // 创建ConsumeQueueExt对象
        	this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    // 加载MappedFile对象
    public boolean load() {
    	// 加载资源
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        // 判断是否执行ConsumeQueueExt加载
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    public void recover() {
    	// 获取该消息队列的所有内存映射文件
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

        	// 只从倒数第3个文件开始，这应该是一个经验值。
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            
            // 获取ByteBuffer对象  | 内存映射文件对应的ByteBuffer
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0; // consumequeue 逻辑大小
            long maxExtAddr = 1;
            while (true) {
            	// 循环验证consumeque包含条目的有效性(如果offset大于0并且size大于0，则表示是一个有效的条目)
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong(); // 8 commitlog中的物理偏移量
                    int size = byteBuffer.getInt(); // 4 该条消息的消息总长度
                    long tagsCode = byteBuffer.getLong(); // 8 tag hashcode

                    // 如果offset大于0并且size大于0，则表示是一个有效的条目，设置consumequeue中有效的
                    if (offset >= 0 && size > 0) {
                    	// 更新mappedFileOffset
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset; // 更新
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                // 如果该consumeque文件中所有条目全部有效，则继续验证下一个文件
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++; // 指向下一个MappedFile文件索引
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                    	// 设置相应变量值
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        // 当前consuemque有效的偏移量
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            // 当前consuemque有效的偏移量
            processOffset += mappedFileOffset;
            
            // 设置setFlushedWhere，setCommittedWhere为当前有效的偏移量
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            
            // 截断无效的consumeque文件。
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // 判断是否扩展读
            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    public long getOffsetInQueueByTime(final long timestamp) {
    	// 返回最近更新的MappedFile对象
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            
            // commitLog对象中最小物理偏移量
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                // 记录高位索引
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                	
                    while (high >= low) {
                    	// 中位偏移量
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        // 获取Long类型数据
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        
                        // 当该phyOffset小于commitLog中最小物理偏移量时
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        // 获取phyOffset所指向的消息存储时间(56索引)
                        long storeTime =
                            this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset; // 找到目标消息
                            break;
                        } else if (storeTime > timestamp) { // 
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    // 对phyOffset索引之后的mappedFile进行删除,循环方式
    // 对offset所在mappedFile文件进行wroteposition、commitPostion、flushPosition进行重置
    // 
    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffet - 1;
        long maxExtAddr = 1;
        while (true) {
        	
        	// 获取数组最后一个元素MappedFile对象
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                // 对每20位单元,进行控制
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong(); // 8
                    int size = byteBuffer.getInt(); // 4
                    long tagsCode = byteBuffer.getLong(); // 8

                    if (0 == i) {
                    	// 当该存储单元物理偏移量 > offset时 ,删除该MappedFile对象
                        if (offset >= phyOffet) {
                        	// 删除MappedFile对象
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                        	// 设置操作索引位置
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset; // 设置最大物理偏移量
                            
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                        	// 当该单元物理偏移量 > phyOffset时,直接就返回
                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset; // 设置最大物理偏移量
                            
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            // 当到达文件末尾时
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    // 获取最后一个MappedFile对象的最后(存储单元)索引号
    // (最近putMessage成功后的消息索引位置offset+size)
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        // 获取最后一个MappedFile对象
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {
        	
        	// 在当前写指针向前推送20字节单位
            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
            	// 获取该存储单元所指向的commitLog物理偏移量(最近putMessage成功的offset)
                long offset = byteBuffer.getLong();
                // 在最近putMessage成功消息的总长度
                int size = byteBuffer.getInt(); 
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }
    
    // 刷新磁盘
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    // 纠正最小逻辑偏移量(基于ConsumeQueue对象)
    public void correctMinOffset(long phyMinOffset) {
    	// 获取第一个MappedFile对象
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        // 当大于phyMinOffset时,更新minLogicOffset属性值
                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = result.getMappedFile().getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    // 获取该consumeQueue最小偏移量(单位:CQ_STORE_UNIT_SIZE)
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                	// 线程睡眠
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            this.maxPhysicOffset = offset;
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    // 获取以startIndex为开始位置的byteBuffer数据
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
        	// 获取offset索引所在的MappedFile对象
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
            	// 在该mappedFile对象中获取以pos开始的数据
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    // 获取在队列中的消息总数
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    // 获取在consumeQueue中最大偏移量(单位:CQ_STORE_UNIT_SIZE)
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    // consumeQueueExt对象不为空时
    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
