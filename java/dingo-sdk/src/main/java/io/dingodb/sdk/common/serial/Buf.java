/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.common.serial;

/**
 * buf接口.
 */
public interface Buf {

    /**
     * 向buf写入一个字节.
     * @param b
     */
    void write(byte b);

    /**
     * 向buf写入一个字节序列.
     * @param b
     */
    void write(byte[] b);

    /**
     * 向b的pos写入length个字节.
     * @param b
     * @param pos
     * @param length
     */
    void write(byte[] b, int pos, int length);

    /**
     * 写入一个int.
     * @param i
     */
    void writeInt(int i);

    /**
     * 写入一个long.
     * @param l
     */
    void writeLong(long l);

    /**
     * 获得当前位置的byte，buffer指针不移动。
     * @return
     */
    byte peek();

    /**
     * 获得当前位置的int，buffer指针不移动
     * @return
     */
    int peekInt();

    /**
     * 获得当前位置的long，buffer指针不移动。
     * @return
     */
    long peekLong();

    /**
     * 读一个byte.
     * @return
     */
    byte read();

    /**
     * 读length个byte.
     * @param length
     * @return
     */
    byte[] read(int length);

    /**
     * 从b的pos位置读取length个byte.
     * @param b
     * @param pos
     * @param length
     */
    void read(byte[] b, int pos, int length);

    /**
     * 读一个int.
     * @return
     */
    int readInt();

    /**
     * 读一个long.
     * @return
     */
    long readLong();

    /**
     * 反向写入一个byte
     * @param b
     */
    void reverseWrite(byte b);

    /**
     * 反向读取一个byte
     * @return
     */
    byte reverseRead();

    /**
     * 反向写入一个int
     * @param i
     */
    void reverseWriteInt(int i);

    /**
     * 反向写入一个int 0
     */
    void reverseWriteInt0();

    /**
     * 反向读取一个int
     * @return
     */
    int reverseReadInt();

    /**
     * 跳过length字节.
     * @param length
     */
    void skip(int length);

    /**
     * 反向跳过length个字节
     * @param length
     */
    void reverseSkip(int length);

    /**
     * 反向跳过一个int
     */
    void reverseSkipInt();

    /**
     * 保证剩余空间能够容纳length个字节
     * @param length
     */
    void ensureRemainder(int length);

    /**
     * 把buf从oldSzie变更为newSize.
     * @param oldSize
     * @param newSize
     */
    void resize(int oldSize, int newSize);

    /**
     * 判断buffer中数据是否读取完毕
     * @return
     */
    boolean isEnd();

    /**
     * 获得buffer中全部字节
     * @return
     */
    byte[] getBytes();
}
