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

package org.apache.rocketmq.filter.util;

/**
 * Wrapper of bytes array, in order to operate single bit easily.
 */
public class BitsArray implements Cloneable {

    private byte[] bytes;
    private int bitLength;

    public static BitsArray create(int bitLength) {
        return new BitsArray(bitLength);
    }

    public static BitsArray create(byte[] bytes, int bitLength) {
        return new BitsArray(bytes, bitLength);
    }

    public static BitsArray create(byte[] bytes) {
        return new BitsArray(bytes);
    }

    // 创建BitsArray对象
    private BitsArray(int bitLength) {
        this.bitLength = bitLength;
        // init bytes 除以8商数
        int temp = bitLength / Byte.SIZE;
        // 除以8余数不为0
        if (bitLength % Byte.SIZE > 0) {
            temp++; // 递增一个字节数
        }
        
        // 创建字节数组
        bytes = new byte[temp];
        for (int i = 0; i < bytes.length; i++) {
        	// 初始化为0
            bytes[i] = (byte) 0x00;
        }
    }

    // 创建BitsArray字节数组对象啊
    private BitsArray(byte[] bytes, int bitLength) {
        if (bytes == null || bytes.length < 1) {
            throw new IllegalArgumentException("Bytes is empty!");
        }

        if (bitLength < 1) {
            throw new IllegalArgumentException("Bit is less than 1.");
        }

        if (bitLength < bytes.length * Byte.SIZE) {
            throw new IllegalArgumentException("BitLength is less than bytes.length() * " + Byte.SIZE);
        }

        // 记录字节数组长度
        this.bytes = new byte[bytes.length];
        // 数组copy
        System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
        
        // 记录比特位长度
        this.bitLength = bitLength;
    }

    // 创建BitArray对象
    private BitsArray(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            throw new IllegalArgumentException("Bytes is empty!");
        }

        // 记录比特位长度与字节数组长度
        this.bitLength = bytes.length * Byte.SIZE;
        this.bytes = new byte[bytes.length];
        
        // 数组copy
        System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
    }

    // 获取比特长度
    public int bitLength() {
        return this.bitLength;
    }

    // 获取字节数组长度
    public int byteLength() {
        return this.bytes.length;
    }

    // 获取字节数组
    public byte[] bytes() {
        return this.bytes;
    }

    // 将两个BitsArray字节数的比特位进行异或运算
    public void xor(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        // 获取两者数组之间的最小值
        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
        	// 1 1 = 0 ; 1 0 = 1 ; 0 0 = 0
        	// 异或运算
            this.bytes[i] = (byte) (this.bytes[i] ^ other.getByte(i));
        }
    }

    public void xor(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        // 获取bitPos位置上的是0: false 1: true
        boolean value = getBit(bitPos);
        
        if (value ^ set) { // 异或为true
            setBit(bitPos, true); // 设置为 1
        } else {
            setBit(bitPos, false); // 位置为0
        }
    }

    // 将两个BitsArray字节数的比特位进行或运算
    public void or(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        // 获取两者数组长度的最小值
        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
        	// 将相应索引上的字节进行或运算
            this.bytes[i] = (byte) (this.bytes[i] | other.getByte(i));
        }
    }

    public void or(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        if (set) {
        	// true,将bitPos比特位上设置为 1
            setBit(bitPos, true);
        }
    }

    // 将两个BitsArray字节数的比特位进行与运算
    public void and(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        // 获取两者数组长度之间的最小值
        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
        	// 将两者数组索引号的字节进行与运算
            this.bytes[i] = (byte) (this.bytes[i] & other.getByte(i));
        }
    }

    public void and(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        if (!set) {
            setBit(bitPos, false);
        }
    }

    // 对相应字节数的数组进行非运算
    public void not(int bitPos) {
        checkBitPosition(bitPos, this);

        setBit(bitPos, !getBit(bitPos));
    }

    public void setBit(int bitPos, boolean set) {
    	// 检查bitPos、this对象有效性
        checkBitPosition(bitPos, this);
        
        // bitPos除以8的商数
        int sub = subscript(bitPos);
        // bitPos除以8的余数
        int pos = position(bitPos);
        
        if (set) {
        	// 在相应bit位上设置为1
            this.bytes[sub] = (byte) (this.bytes[sub] | pos);
        } else {
        	// 在相应bit位上设置为0
            this.bytes[sub] = (byte) (this.bytes[sub] & ~pos);
        }
    }

    public void setByte(int bytePos, byte set) {
        checkBytePosition(bytePos, this);
        
        // 设置字节数组
        this.bytes[bytePos] = set;
    }

    // = 0 : false   = 1 : true
    public boolean getBit(int bitPos) {
        checkBitPosition(bitPos, this);

        return (this.bytes[subscript(bitPos)] & position(bitPos)) != 0;
    }

    // 获取bytePos上的字节数据
    public byte getByte(int bytePos) {
        checkBytePosition(bytePos, this);

        return this.bytes[bytePos];
    }

    // 判断索引为分配在那个字节(除以8的商数)上
    protected int subscript(int bitPos) {
        return bitPos / Byte.SIZE;
    }

    // 判断索引在字节的那个bit位上
    protected int position(int bitPos) {
        return 1 << bitPos % Byte.SIZE;
    }

    protected void checkBytePosition(int bytePos, BitsArray bitsArray) {
        checkInitialized(bitsArray);
        if (bytePos > bitsArray.byteLength()) {
            throw new IllegalArgumentException("BytePos is greater than " + bytes.length);
        }
        if (bytePos < 0) {
            throw new IllegalArgumentException("BytePos is less than 0");
        }
    }

    protected void checkBitPosition(int bitPos, BitsArray bitsArray) {
        // 检查BitsArray对象
    	checkInitialized(bitsArray);
        // 校验输入参数的合法性
        if (bitPos > bitsArray.bitLength()) {
            throw new IllegalArgumentException("BitPos is greater than " + bitLength);
        }
        if (bitPos < 0) {
            throw new IllegalArgumentException("BitPos is less than 0");
        }
    }

    protected void checkInitialized(BitsArray bitsArray) {
    	// 判断字节数组是否为空
        if (bitsArray.bytes() == null) {
            throw new RuntimeException("Not initialized!");
        }
    }

    // 创建新的BitsArray对象
    public BitsArray clone() {
        byte[] clone = new byte[this.byteLength()];

        // 复制字节数组对象
        System.arraycopy(this.bytes, 0, clone, 0, this.byteLength());

        // 创建BitsArray对象
        return create(clone, bitLength());
    }

    @Override // 返回String对象
    public String toString() {
        if (this.bytes == null) {
            return "null";
        }
        StringBuilder stringBuilder = new StringBuilder(this.bytes.length * Byte.SIZE);
        for (int i = this.bytes.length - 1; i >= 0; i--) {

            int j = Byte.SIZE - 1;
            if (i == this.bytes.length - 1 && this.bitLength % Byte.SIZE > 0) {
                // not full byte
                j = this.bitLength % Byte.SIZE;
            }

            for (; j >= 0; j--) {

                byte mask = (byte) (1 << j);
                if ((this.bytes[i] & mask) == mask) {
                    stringBuilder.append("1");
                } else {
                    stringBuilder.append("0");
                }
            }
            if (i % 8 == 0) {
                stringBuilder.append("\n");
            }
        }

        return stringBuilder.toString();
    }
}
