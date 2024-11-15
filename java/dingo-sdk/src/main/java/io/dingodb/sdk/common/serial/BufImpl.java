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

public class BufImpl implements Buf {

    /**
     * 内部buffer.
     */
    private byte[] buf;

    /**
     * 前向当前位置.
     */
    private int forwardPos;

    /**
     * 反向当前位置.
     */
    private int reversePos;

    /**
     * 创建bufSize大小的buffer.
     * @param bufSize 待补充.
     */
    public BufImpl(int bufSize) {
        this.buf = new byte[bufSize];
        this.forwardPos = 0;
        this.reversePos = bufSize - 1;
    }

    /**
     * 从字节数组keyBuf创建对象.
     * @param keyBuf 待补充.
     */
    public BufImpl(byte[] keyBuf) {
        this.buf = keyBuf;
        this.forwardPos = 0;
        this.reversePos = keyBuf.length - 1;
    }

    /**
     * 写入buffer一个字节b.
     * @param b 待补充.
     */
    @Override
    public void write(byte b) {
        buf[forwardPos++] = b;
    }

    /**
     * 把b写入buffer.
     * @param b 待补充.
     */
    @Override
    public void write(byte[] b) {
        System.arraycopy(b, 0, buf, forwardPos, b.length);
        forwardPos += b.length;
    }

    /**
     * 把b的pos位置的length个字节写入buffer.
     * @param b 待补充.
     * @param pos 待补充.
     * @param length 待补充.
     */
    @Override
    public void write(byte[] b, int pos, int length) {
        System.arraycopy(b, pos, buf, forwardPos, length);
        forwardPos += length;
    }

    /**
     * 写入一个int.
     * @param i 待补充.
     */
    @Override
    public void writeInt(int i) {
        buf[forwardPos++] = (byte) (i >>> 24);
        buf[forwardPos++] = (byte) (i >>> 16);
        buf[forwardPos++] = (byte) (i >>> 8);
        buf[forwardPos++] = (byte) i;
    }

    /**
     * 写入一个long.
     * @param l 待补充.
     */
    @Override
    public void writeLong(long l) {
        buf[forwardPos++] = (byte) (l >>> 56);
        buf[forwardPos++] = (byte) (l >>> 48);
        buf[forwardPos++] = (byte) (l >>> 40);
        buf[forwardPos++] = (byte) (l >>> 32);
        buf[forwardPos++] = (byte) (l >>> 24);
        buf[forwardPos++] = (byte) (l >>> 16);
        buf[forwardPos++] = (byte) (l >>> 8);
        buf[forwardPos++] = (byte) l;
    }

    /**
     * 获得buffer当前的字节,buffer指针不变.
     * @return 待补充.
     */
    @Override
    public byte peek() {
        return buf[forwardPos];
    }

    /**
     * 获得当前位置的int,buffer指针不变.
     * @return 待补充.
     */
    @Override
    public int peekInt() {
        return (
            ((buf[forwardPos]     & 0xFF) << 24)
          | ((buf[forwardPos + 1] & 0xFF) << 16)
          | ((buf[forwardPos + 2] & 0xFF) << 8 )
          | ( buf[forwardPos + 3] & 0xFF       )
        );
    }

    /**
     * 获得当前位置的long,buffer指针不变.
     * @return 待补充.
     */
    @Override
    public long peekLong() {
        long l = buf[forwardPos] & 0xFF;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf[forwardPos + i + 1] & 0xFF;
        }
        return l;
    }

    /**
     * 读取一个byte，指针前移.
     * @return 待补充.
     */
    @Override
    public byte read() {
        return buf[forwardPos++];
    }

    /**
     * 读取length个byte，指针前移.
     * @return 待补充.
     */
    @Override
    public byte[] read(int length) {
        byte[] b = new byte[length];
        System.arraycopy(buf, forwardPos, b, 0, length);
        forwardPos += length;
        return b;
    }

    /**
     * 读取length个byte到b的pos位置.
     * @return 待补充.
     */
    @Override
    public void read(byte[] b, int pos, int length) {
        System.arraycopy(buf, forwardPos, b, pos, length);
        forwardPos += length;
    }

    /**
     * 读取一个int.
     * @return 待补充.
     */
    @Override
    public int readInt() {
        return (((buf[forwardPos++] & 0xFF) << 24)
                | ((buf[forwardPos++] & 0xFF) << 16)
                | ((buf[forwardPos++] & 0xFF) << 8)
                | buf[forwardPos++] & 0xFF);
    }

    /**
     * 读取一个long，指针前移。
     * @return
     */
    @Override
    public long readLong() {
        long l = buf[forwardPos++]  & 0xFF;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf[forwardPos++] & 0xFF;
        }
        return l;
    }

    /**
     * 反向写入一个字节b。
     * @param b
     */
    @Override
    public void reverseWrite(byte b) {
        buf[reversePos--] = b;
    }

    /**
     * 反向读取一个字节。
     * @return
     */
    @Override
    public byte reverseRead() {
        return buf[reversePos--];
    }

    /**
     * 反向写入一个int
     * @param i
     */
    @Override
    public void reverseWriteInt(int i) {
        buf[reversePos--] = (byte) (i >>> 24);
        buf[reversePos--] = (byte) (i >>> 16);
        buf[reversePos--] = (byte) (i >>> 8);
        buf[reversePos--] = (byte) i;
    }

    /**
     * 反向写入int 0
     */
    @Override
    public void reverseWriteInt0() {
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
    }

    /**
     * 反向读取一个int。
     * @return
     */
    @Override
    public int reverseReadInt() {
        return (((buf[reversePos--] & 0xFF) << 24)
                | ((buf[reversePos--] & 0xFF) << 16)
                | ((buf[reversePos--] & 0xFF) << 8)
                | buf[reversePos--] & 0xFF);
    }

    /**
     * 跳过length个字节
     * @param length
     */
    @Override
    public void skip(int length) {
        forwardPos += length;
    }

    /**
     * 反向跳过length个字节
     * @param length
     */
    @Override
    public void reverseSkip(int length) {
        reversePos -= length;
    }

    /**
     * 反向跳过一个int。
     */
    @Override
    public void reverseSkipInt() {
        reversePos -= 4;
    }

    /**
     * 确保剩余空间大于等于length个字节.
     * @param length
     */
    @Override
    public void ensureRemainder(int length) {
        if ((forwardPos + length - 1) > reversePos) {
            int newSize;
            if (length > Config.SCALE) {
                newSize = buf.length + length;
            } else {
                newSize = buf.length + Config.SCALE;
            }

            byte[] newBuf = new byte[newSize];
            System.arraycopy(buf, 0, newBuf, 0, forwardPos);
            int reverseSize = buf.length - reversePos - 1;
            System.arraycopy(buf, reversePos + 1, newBuf, newSize - reverseSize, reverseSize);
            reversePos = newSize - reverseSize - 1;
            buf = newBuf;
        }
    }

    /**
     * 重新调整buffer大小从oldsize到newsize。
     * @param oldSize
     * @param newSize
     */
    @Override
    public void resize(int oldSize, int newSize) {
        if (oldSize != newSize) {
            byte[] newBuf = new byte[buf.length + newSize - oldSize];
            System.arraycopy(buf, 0, newBuf, 0, forwardPos);
            int backPos = forwardPos + oldSize;
            System.arraycopy(buf, backPos, newBuf, forwardPos + newSize, buf.length - backPos);
            buf = newBuf;
            reversePos += (newSize - oldSize);
        }
    }

    /**
     * 判断是否buffer读取结束。
     * @return
     */
    @Override
    public boolean isEnd() {
        return (reversePos - forwardPos + 1) == 0;
    }

    /**
     * 获得buffer中全部字节
     * @return
     */
    @Override
    public byte[] getBytes() {
        int emptySize = reversePos - forwardPos + 1;
        if (emptySize == 0) {
            return buf;
        }
        if (emptySize > 0) {
            int finalSize = buf.length - emptySize;
            byte[] finalBuf = new byte[finalSize];
            System.arraycopy(buf, 0, finalBuf, 0, forwardPos);
            System.arraycopy(buf, reversePos + 1, finalBuf, forwardPos, finalSize - forwardPos);
            buf = finalBuf;
            reversePos = forwardPos - 1;
            return buf;
        }
        if (emptySize < 0) {
            throw new RuntimeException("Wrong Key Buf");
        }
        return null;
    }
}
