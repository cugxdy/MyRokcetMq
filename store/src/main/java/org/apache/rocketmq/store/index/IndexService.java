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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// IndexService服务提供类
public class IndexService {
	
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    // 最大重试创建次数(3)
    private static final int MAX_TRY_IDX_CREATE = 3;
    
    // 消息配置类
    private final DefaultMessageStore defaultMessageStore;
    
    // 最大哈希槽数目
    private final int hashSlotNum; 
    
    // 最大索引数目
    private final int indexNum;
    
    // 存储路径(default = C:\Users\Administrator\store\index)
    private final String storePath;
    
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    
    // 读写锁
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
    	
        this.defaultMessageStore = store;
        // 最大哈希槽数目                 hash槽数量,默认5百万个
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        // 最大索引数目                    index条目个数,默认为 2千万个
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        
        // 存储路径                            index存储路径,默认为:/rocket_home/store/index
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    // 从配置路径中去加载文件
    public boolean load(final boolean lastExitOK) {
    	// 目录文件(default = C:\Users\Administrator\store\index)
        File dir = new File(this.storePath); 
        
        // 获取目录下的所有文件
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            // 遍历文件夹
            for (File file : files) {
                try {
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    // 从文件中去注入IndexHeader属性值
                    f.load();

                    if (!lastExitOK) {
                    	// 判断是否为异常退出的情况
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    // 日志记录并将IndexFile文件加入到ArrayList<IndexFile>中
                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    // 根据索引删除已存在的IndexFile文件
    // 删除物理偏移量小于offset的所有IndexFile文件(只有一个除外)
    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
        	// 读锁上锁
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            // 获取第一个IndexFile文件中的最大物理偏移量
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
            	// 转换成数组形式
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                // 当最大物理偏移量小于offset时,将其加入待删除list中
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    // 依据files对象删除IndexFile文件
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                	// 删除IndexFile并将其从list中删除元素
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    // 如果删除失败,日志记录并退出
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
            	// 释放写锁
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    // 获取写锁并将所有IndexFile文件进行删除
    public void destroy() {
        try {
        	// 获取写锁
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
        	// 释放写锁
            this.readWriteLock.writeLock().unlock();
        }
    }

    // 查询Offset作为List对象
    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        // 计算最大获取消息索引数目
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
        	// 获取读锁
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                	// 获取第i-1个IndexFile文件
                    IndexFile f = this.indexFileList.get(i - 1);
                    
                    // 获取最后一个IndexFile文件的最大时间戳与最大物理偏移量
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    // 判断f是否部分或者全部处于begin-end区间中
                    if (f.isTimeMatched(begin, end)) {
                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    // 已到达最后一个需要查询的IndexFile文件
                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    // 如果已到达最大数目,直接就返回
                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
        	// 读锁
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    // 返回topic#key字符串
    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    public void buildIndex(DispatchRequest req) {
    	// 创建或获取当前写入的IndexFile
    	// 两种情况:
    	// 1、当前已存在但是未写满的Indexfile文件
    	// 1、当前最大物理偏移量IndexFile已满,新建IndexFile文件
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            
            // 获取Topic字符串
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            
            // 如果indexfile中的最大偏移量大于该消息的commitlog offset，忽略本次构建
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            // 判断消息类型
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            // 将消息中的keys,uniq_keys写入index文件中。
            if (req.getUniqKey() != null) {
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            // 将消息中的keys,uniq_keys写入index文件中。
            if (keys != null && keys.length() > 0) {
            	// 以空格作为分隔符进行分割
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                    	// 对每一个key存入IndexFile文件中
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    // 将索引存入IndexFile文件中
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        // 重试三次将msg存入IndexFile文件中
    	for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            // 如果为null,直接返回
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

    	// 返回IndexFile文件
        return indexFile;
    }

    /**
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    // 创建IndexFile文件
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        // 重试创建IndexFile文件,最大次数为3次
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            
        	indexFile = this.getAndCreateLastIndexFile();
            // 创建成功返回IndexFile文件
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
        	// IndexFile文件创建失败
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    // 创建IndexFile文件
    public IndexFile getAndCreateLastIndexFile() {
    	
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
            	// 获取最新IndexFile文件
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                // 当前IndexFile未写满时
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                } else {
                	// 当前IndexFile已满时
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        if (indexFile == null) {
            try {
            	// 返回字符串格式为20190829143254149
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                // 创建IndexFile文件,并设置最小时间戳以及最小物理偏移量
                // lastUpdateIndexTimestamp : 上一个IndexFile最大时间戳
                // lastUpdateEndPhyOffset : 上一个IndexFile最大物理偏移量
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                
                // 将新建的IndexFile文件添加进List中
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
            	// 释放写锁
                this.readWriteLock.writeLock().unlock();
            }

            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                // 异步创建Thread进行IndexFile文件刷盘操作
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                // 启动线程
                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    // 对IndexFile文件进行刷盘操作
    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        // 判断IndexFile文件是否已满
        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        // IndexFile文件刷盘操作
        f.flush();

        // 进行文件检查点刷盘操作
        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
