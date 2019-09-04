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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexHeader {
	
	// 它记录了IndexHeader所占用字节数
    public static final int INDEX_HEADER_SIZE = 40;
    
    // 以下static变量记录了indexHeader中的索引开始位置
    private static int beginTimestampIndex = 0;
    private static int endTimestampIndex = 8;
    private static int beginPhyoffsetIndex = 16;
    private static int endPhyoffsetIndex = 24;
    private static int hashSlotcountIndex = 32;
    private static int indexCountIndex = 36;
    
    
    private final ByteBuffer byteBuffer;
    
    // 该索引文件中包含消息的最小存储时间
    private AtomicLong beginTimestamp = new AtomicLong(0);
    
    // 该索引文件中包含消息的最大存储时间
    private AtomicLong endTimestamp = new AtomicLong(0);
    
    // 该索引文件中包含消息的最小物理偏移量(CommitLog文件偏移量)
    private AtomicLong beginPhyOffset = new AtomicLong(0);
    
    // 该索引文件中包含消息的最大物理偏移量(CommitLog文件偏移量)
    private AtomicLong endPhyOffset = new AtomicLong(0);
    
    // hashslot个数
    private AtomicInteger hashSlotCount = new AtomicInteger(0);

    // Index条目列表当前已使用的个数,Index条目在Index条目列表中按顺序存储
    private AtomicInteger indexCount = new AtomicInteger(1);

    // 创建IndexHeader对象
    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public void load() {
    	// 获取IndexHeader中各个字段的属性值,从字节数组中去获取
        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));

        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }

    // 更新IndexHeader相应属性值
    public void updateByteBuffer() {
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    // 获取该索引文件中包含消息的最小存储时间
    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    // 设置该索引文件中包含消息的最小存储时间
    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    // 获取该索引文件中包含消息的最大存储时间
    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    // 设置该索引文件中包含消息的最大存储时间
    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    // 获取该索引文件中包含消息的最小物理偏移量(CommitLog文件偏移量)
    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }

    // 设置该索引文件中包含消息的最小物理偏移量(CommitLog文件偏移量)
    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    // 获取该索引文件中包含消息的最大物理偏移量(CommitLog文件偏移量)
    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    // 设置该索引文件中包含消息的最大物理偏移量(CommitLog文件偏移量)
    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    // 获取hashslot个数
    public AtomicInteger getHashSlotCount() {
        return hashSlotCount;
    }

    // 设置hashslot个数
    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    // 获取Index条目列表当前已使用的个数,Index条目在Index条目列表中按顺序存储
    public int getIndexCount() {
        return indexCount.get();
    }

    // 设置Index条目列表当前已使用的个数,Index条目在Index条目列表中按顺序存储
    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }
}
