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

package org.apache.rocketmq.broker.transaction;

// 事务记录对象, 相当于数据库每行所对应Bean数据对象
public class TransactionRecord {
	
    // Commit Log Offset
    private long offset; // commitLog offset 偏移量
    
    // 生产者名称
    private String producerGroup;

    public long getOffset() {
        return offset;
    }

    // 设置commitLog索引号
    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    // 设置生成者名称
    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
