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

public enum PutMessageStatus {
    PUT_OK, // 成功
    FLUSH_DISK_TIMEOUT, // 写磁盘超时
    FLUSH_SLAVE_TIMEOUT, // 同步从服务器失败
    SLAVE_NOT_AVAILABLE, // 从服务器不可获得
    SERVICE_NOT_AVAILABLE, // 服务不可用
    CREATE_MAPEDFILE_FAILED, // 创建Map映射文件失败
    MESSAGE_ILLEGAL, // 
    PROPERTIES_SIZE_EXCEEDED, // 
    OS_PAGECACHE_BUSY, // 系统繁忙
    UNKNOWN_ERROR,  // 未知错误
}
