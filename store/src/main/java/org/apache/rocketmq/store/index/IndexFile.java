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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    
    // 哈希槽大小(占用字节数)
    private static int hashSlotSize = 4;
    
    // 每条indexFile条目占用字节数
    private static int indexSize = 20; 
    
    private static int invalidIndex = 0;
    
    // 哈希槽计数
    private final int hashSlotNum;
    
    // Index条目计数
    private final int indexNum;
    
    // 内存映射文件(java Nio)
    private final MappedFile mappedFile;
    
    private final FileChannel fileChannel;
    
    // ByteBuffer对象
    private final MappedByteBuffer mappedByteBuffer;
    
    // IndexHeader,每一个indexfile的头部信息
    private final IndexHeader indexHeader; 

    // 创建IndexFile对象
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        
    	// 计算文件大小(40 + hashSlotNum * 4 + indexNum * 20)
    	int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        
    	this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        
        // 设置槽slot计数与Index计数
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        
        // 创建IndexHeader对象,slice共享内存区,但是却拥有独立的position、limit、capacity属性值
        this.indexHeader = new IndexHeader(byteBuffer);

        // 设置最大与最小物理偏移量
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        } 

        // 设置该索引文件中包含消息的最小与最大存储时间
        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    // 获取文件名
    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    // 设置IndexHeader相关属性值
    public void load() {
        this.indexHeader.load();
    }

    
    public void flush() {
        long beginTime = System.currentTimeMillis();
        // 引用计数算法使用
        if (this.mappedFile.hold()) {
        	// 刷新内存映射文件中的IndexFile属性值
            this.indexHeader.updateByteBuffer();
            // 刷盘
            this.mappedByteBuffer.force();
            // 释放引用计数
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    // 判断是否可写
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    // 销毁MappedFile文件
    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    // phyOffset:消息存储在commitlog的偏移量;
    // storeTimestamp:消息存入commitlog的时间戳
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        
    	// 如果目前index file存储的条目数小于允许的条目数，则存入当前文件中，
    	// 如果超出，则返回false,表示存入失败，IndexService中有重试机制，默认重试3次
    	if (this.indexHeader.getIndexCount() < this.indexNum) {
            
    		// 获取哈希值
    		int keyHash = indexKeyHashMethod(key);
            
            // 先获取key的hashcode，然后用hashcode和 hashSlotNum取模，得到该key所在的hashslot下标，hashSlotNum默认500万个
            int slotPos = keyHash % this.hashSlotNum;
            
            // 根据key所算出来的hashslot的下标计算出绝对位置，
            // 从这里可以看出端倪：IndexFile的文件布局:文件头(IndexFileHeader 20个字节) + (hashSlotNum * 4)
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
            	// 读取key所在hashslot下标处的值(4个字节)，如果小于0或超过当前包含的indexCount，则设置为0；
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // ms单位
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                // s单位
                timeDiff = timeDiff / 1000;
                
                // 计算消息的存储时间与当前IndexFile存放的最小时间差额(单位为秒）
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }  

                // 计算该key存放的条目的起始位置,等于=文件头(IndexFileHeader 20个字节) + (hashSlotNum*4) + IndexSize(一个条目20个字节)*当前存放的条目数量！
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                // 填充IndexFile条目,4字节(hashcode) + 8字节(commitlog offset) + 4字节(commitlog存储时间与indexfile第一个条目的时间差，单位秒) + 4字节(同hashcode的上一个的位置，0表示没有上一个)
                
                this.mappedByteBuffer.putInt(absIndexPos, keyHash); // 哈希值
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset); // commlog索引
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff); // 时间戳
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue); // Index位置

                // 将当前先添加的条目的位置，存入到key hashcode对应的hash槽，也就是该字段里面存放的是该hashcode最新的条目(如果产生hash冲突,不同的key,hashcode相同。
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                
                // 更新IndexFile头部相关字段，比如最小时间，当前最大时间等。
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 递增哈希槽与Index条目计数
                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                    	// 是否文件锁
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
        	// 记录IndexFile文件中Index计数
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    // 获取key值的hashCode值
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    // 获取该索引文件中包含消息的最小存储时间
    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    // 获取该索引文件中包含消息的最大存储时间
    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    // 获取该索引文件中包含消息的最大物理偏移量(CommitLog文件偏移量)
    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    // 存在三种情况返回true
    // 1、begin < Ibegin < Iend < end
    // 2、Ibegin < begin < Iend
    // 3、Ibegin < end < Iend
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

	// phyOffsets : 符合查找条件的物理偏移量(commitlog文件中的偏移量)
	// key : 索引键值，待查找的key
	// start : 开始时间戳(毫秒)
	// end : 结束时间戳(毫秒)
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
    	
        if (this.mappedFile.hold()) {
        	
        	// 根据key算出hashcode,然后定位到hash槽的位置。
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            
            // 获取hash槽所在位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取Index条目所在位置
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // 		fileLock.release();
                // 		fileLock = null;
                // }

                // 如果该位置存储的值小于0,或者大于当前indexCount的值，则视为无效，也就是该hashcode值并没有对应的index条码存储，如果等于0或小于当前条目的大小，则表示至少存储了一个。
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                	/**
                	 * slotValue <= invalidIndex:
                	 */
                } else {
                	
                	// 找到条目内容，然后与查询条件进行匹配，如果符合，则将物理偏移量加入到phyOffsets中，否则，继续寻找。
                    // 这里存在对链式冲突的解决方法
                	for (int nextIndexToRead = slotValue; ; ) {
                		
                		// 已达到允许获取最大数目
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        // 获取哈希值
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        
                        // 获取物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        // 获取存储时间戳
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        
                        // 获取此Index上一个Index条目
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        // s(单位)
                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        
                        // 是否在最大与最小之间
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        // 比较两者对象之间HashCode
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        // 对prevIndexRead进行判断,判断其指向的位置是否有效
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
