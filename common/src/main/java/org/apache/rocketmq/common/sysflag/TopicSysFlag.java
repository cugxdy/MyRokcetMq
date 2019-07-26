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
package org.apache.rocketmq.common.sysflag;

public class TopicSysFlag {

	// 0000 0001 = FLAG_UNIT
    private final static int FLAG_UNIT = 0x1 << 0;

    // 0000 0010 = FLAG_UNIT_SUB
    private final static int FLAG_UNIT_SUB = 0x1 << 1;

    public static int buildSysFlag(final boolean unit, final boolean hasUnitSub) {
        int sysFlag = 0;

        // 设置FLAG_UNIT标识位
        if (unit) {
            sysFlag |= FLAG_UNIT;
        }

        // 设置FLAG_UNIT_SUB标识位
        if (hasUnitSub) {
            sysFlag |= FLAG_UNIT_SUB;
        }

        return sysFlag;
    }

    // 设置FLAG_UNIT标识位
    // 0000 0000 | 0000 0001 = 0000 0001
    // 0000 0001 | 0000 0001 = 0000 0001
    public static int setUnitFlag(final int sysFlag) {
        return sysFlag | FLAG_UNIT;
    }
    

    // 擦除FLAG_UNIT标识位
    // ~FLAG_UNIT = 1111 1110
    // 0000 0000 & 1111 1110 = 0000 0000
    // 0000 0001 & 1111 1110 = 0000 0000
    public static int clearUnitFlag(final int sysFlag) {
        return sysFlag & (~FLAG_UNIT);
    }

    public static boolean hasUnitFlag(final int sysFlag) {
        return (sysFlag & FLAG_UNIT) == FLAG_UNIT;
    }

    public static int setUnitSubFlag(final int sysFlag) {
        return sysFlag | FLAG_UNIT_SUB;
    }

    public static int clearUnitSubFlag(final int sysFlag) {
        return sysFlag & (~FLAG_UNIT_SUB);
    }

    public static boolean hasUnitSubFlag(final int sysFlag) {
        return (sysFlag & FLAG_UNIT_SUB) == FLAG_UNIT_SUB;
    }

    public static void main(String[] args) {
    }
}
