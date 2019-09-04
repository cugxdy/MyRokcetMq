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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
public class MappedFile extends ReferenceResource {
	
	// 默认页大小为4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    
    // JVM中映射的虚拟内存总大小  
    // 类变量，所有MappedFile实例已使用字节总数
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    
    // JVM中mmap的数量   
    // 类变量,MappedFile个数。
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    
    // 当前写文件的位置
    // 当前MappedFile对象当前写指针(position)
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    
    //ADD BY ChenYang
    // 当前提交的指针，个人觉得是预先申请这个位置，数据可能还没有真正写入到Buffer中，该值可以大于wrotePosition(待验证)
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    
    // 记录刷新的位置 当前刷写到磁盘的指针
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    
    // 映射文件的大小
    protected int fileSize;
    
    // 映射的fileChannel对象
    protected FileChannel fileChannel;
    
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    // 存入磁盘的缓存区
    protected ByteBuffer writeBuffer = null;
    
    protected TransientStorePool transientStorePool = null;
    
    // 映射的文件名
    private String fileName;
    
    // 映射的起始偏移量
    private long fileFromOffset;
    
    // 映射的文件
    private File file;
    
    // 映射的内存对象
    private MappedByteBuffer mappedByteBuffer;
   
    // 最后一条消息保存时间  
    private volatile long storeTimestamp = 0;
    
    // 是不是第一个创建的(即在MappedFileQueue中CopyOnWriteArrayList第一个元素)
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    // 初始化MappedFile文件
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    
    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    // 创建父目录路径
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
            	// 创建目录
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    // 对堆外内存进行clean操作
    public static void clean(final ByteBuffer buffer) {
    	// 堆内存,容量不为0时,执行clean方法
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        // 对DirectBuffer对象执行cleaner与clean方法
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    // 调用指定方法
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        // 调用doPrivileged方法,java安全性保证
    	return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                	// 获取指定方法名与参数的Method对象
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    // 调用目标方法
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    // 获取指定方法Method(方法名与参数)
    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    // 指定特定方法,默认为viewedBuffer方法,也可能是attachment方法
    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        // 调用methodName方法
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    // 获取Map文件映射对象数目
    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    // 获取总共虚拟内存总数
    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    // 初始化MappedFile对象
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 获取writeBuffer对象
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    // 初始化文件名与文件大小(初始化MappedFile对象)
    @SuppressWarnings("resource")
	private void init(final String fileName, final int fileSize) throws IOException {
        
    	this.fileName = fileName;
        this.fileSize = fileSize;
        // 创建文件对象
        this.file = new File(fileName);
        
        // 文件偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        // 确保路径存在
        ensureDirOK(this.file.getParent());

        try {
        	
        	// 获取文件Chanel对象(以读写模式获取FileChannel对象)
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            
            // 建立文件映射对象
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            
            // 更新文件
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
            	// 关闭文件Channel对象
                this.fileChannel.close();
            }
        }
    }

    // 返回文件最后被修改的时间
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    // 获取文件大小
    public int getFileSize() {
        return fileSize;
    }

    // 获取FileChannel对象
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    // 向byteMapper文件中填写bytebuffer对象
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    // 向MappedFile中写入数据
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        
        // 找出当前要的写入位置
        int currentPos = this.wrotePosition.get();

        // 如果当前位置小于等于文件大小，则说明剩余空间足够写入。
        if (currentPos < this.fileSize) {
        	// 返回新的缓存区,共享数据,即byteBuffer修改能改变writeBuffer的内容
        	// 但是有各自的limit、position、capital
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            
            // 设置开始写入的位置(写模式)
            byteBuffer.position(currentPos);
            AppendMessageResult result = null;
            // 当为MessageExtBrokerInner类型时,进行数据持久化
            // 根据消息类型，是批量消息还是单个消息，进入相应的处理
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
            	// 返回类型错误
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            
            this.wrotePosition.addAndGet(result.getWroteBytes());
            // 更新存储时间戳(最新时间戳)
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    // 获取整个mapp文件偏移量(即文件名)
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    // 将字节数组写入MappedFile文件中
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        // 判断可写空间是否满足需求
        if ((currentPos + data.length) <= this.fileSize) {
            try {
            	// 写入bytebuffer对象
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新写索引
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    // 将字节数组写入MappedFile文件中
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        // 判断可写空间是否满足需求
        if ((currentPos + length) <= this.fileSize) {
            try {
            	// 写入bytebuffer对象
                this.fileChannel.position(currentPos);
                // 将字节数组指定区间中的数组写入到FileChannel对象中
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新写索引
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    // 刷新操作
    public int flush(final int flushLeastPages) {
    	// 判断是否能够进行刷盘操作
        if (this.isAbleToFlush(flushLeastPages)) {
        	// 引用计数器>0
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                    	// 使用force进行刷盘操作
                        this.fileChannel.force(false);
                    } else {
                    	// 刷新到内存中
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                // 记录刷新内存指针
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                // 设置写位置
                this.flushedPosition.set(getReadPosition());
            }
        }
        // 获取刷新指针
        return this.getFlushedPosition();
    }

    // 堆外内存向FileChannel中写入数据
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            // 不需提交数据至文件中
        	return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                // 引用计数递减
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        // 设置已提交索引
        return this.committedPosition.get();
    }

    // 将writeBuffer中的数据写入文件中
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        // 判断是否存在待写入数据
        if (writePos - this.committedPosition.get() > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 定义待写数据区间
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                
                // 向fileChannel中写入数据
                this.fileChannel.position(lastCommittedPosition);
                // 将指定区间[lastCommittedPosition,writePos]下的数据内容存入磁盘中
                this.fileChannel.write(byteBuffer);
                
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    // 判断是否能够进行flush操作
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        
        // 1、使用堆外内存,就从committedPosition中获取
        // 2、使用堆内存,就从wrotePosition中获取
        int write = getReadPosition();

        // 文件已满进行刷盘
        if (this.isFull()) {
            return true;
        }

        // 使用(已写字节数-已提交字节数)来判断是否剩余内存足够
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // 只要存在未提交数据就返回true
        return write > flush;
    }

    // 判断是否能够进行commit操作
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        // 已满时,直接进行提交
        if (this.isFull()) {
            return true;
        }

        // 计算未commit数据是否满足commitLeastPages页数
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    // 获取刷盘指针
    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    // 设置刷盘指针
    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    // 是否存在剩余空间:true = 文件已满
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    // 读取pos与size之间的bytebuffer对象
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        
        // 判断是否能够获取指定的数据
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                
                
                // byteBufferNew中是原ByteBuffer对象中以pos为开始位置的长度为size的数据
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                // 创建SelectMappedBufferResult对象
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    // 获取从pos索引开始的bytebuffer对象
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        // 判断是否具有可获得的bytebuffer对象
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                
                // 获取读取的大小
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                // 创建SelectMappedBufferResult对象
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
    	// 正处于使用状态中
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        // 已处于shutdown状态中
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        // 堆外内存释放
        clean(this.mappedByteBuffer);
        
        // 设置虚拟内存大小
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
            	// 关闭文件Channel对象
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                // 设置文件开始删除时间
                long beginTime = System.currentTimeMillis();
                
                // 删除文件
                boolean result = this.file.delete();
                
                // 日志记录(引用计数,文件删除结果,写指针,刷盘指针)
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    // 获取写指针
    public int getWrotePosition() {
        return wrotePosition.get();
    }

    // 设置写指针
    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    // 获取已写索引
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
    	// 获取当前时间
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        
        // 获取当前时间
        long time = System.currentTimeMillis();
        // j:记录页计数
        // i:每页起始指针
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // 同步刷盘
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // 每经历4M时
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                	// 线程停顿一下
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        // 同步刷盘
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    // 获取锁资源
    @SuppressWarnings("restriction")
	public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    // 释放锁资源
    @SuppressWarnings("restriction")
	public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
