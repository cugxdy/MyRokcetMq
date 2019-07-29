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

    private BitsArray(int bitLength) {
        this.bitLength = bitLength;
        // init bytes
        int temp = bitLength / Byte.SIZE;
        if (bitLength % Byte.SIZE > 0) {
            temp++;
        }
        bytes = new byte[temp];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) 0x00;
        }
    }

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

        this.bytes = new byte[bytes.length];
        System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
        this.bitLength = bitLength;
    }

    private BitsArray(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            throw new IllegalArgumentException("Bytes is empty!");
        }

        this.bitLength = bytes.length * Byte.SIZE;
        this.bytes = new byte[bytes.length];
        System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
    }

    public int bitLength() {
        return this.bitLength;
    }

    public int byteLength() {
        return this.bytes.length;
    }

    public byte[] bytes() {
        return this.bytes;
    }

    public void xor(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
            this.bytes[i] = (byte) (this.bytes[i] ^ other.getByte(i));
        }
    }

    public void xor(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        boolean value = getBit(bitPos);
        if (value ^ set) {
            setBit(bitPos, true);
        } else {
            setBit(bitPos, false);
        }
    }

    public void or(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
            this.bytes[i] = (byte) (this.bytes[i] | other.getByte(i));
        }
    }

    public void or(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        if (set) {
            setBit(bitPos, true);
        }
    }

    public void and(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
            this.bytes[i] = (byte) (this.bytes[i] & other.getByte(i));
        }
    }

    public void and(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        if (!set) {
            setBit(bitPos, false);
        }
    }

    public void not(int bitPos) {
        checkBitPosition(bitPos, this);

        setBit(bitPos, !getBit(bitPos));
    }

    public void setBit(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);
        int sub = subscript(bitPos);
        int pos = position(bitPos);
        if (set) {
            this.bytes[sub] = (byte) (this.bytes[sub] | pos);
        } else {
            this.bytes[sub] = (byte) (this.bytes[sub] & ~pos);
        }
    }

    public void setByte(int bytePos, byte set) {
        checkBytePosition(bytePos, this);

        this.bytes[bytePos] = set;
    }

    // = 0 : true   = 1 : false
    public boolean getBit(int bitPos) {
        checkBitPosition(bitPos, this);

        return (this.bytes[subscript(bitPos)] & position(bitPos)) != 0;
    }

    public byte getByte(int bytePos) {
        checkBytePosition(bytePos, this);

        return this.bytes[bytePos];
    }

    // 判断索引为分配在那个字节上
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

    @Override
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
