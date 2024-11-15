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

package io.dingodb.sdk.common.serial.schema;

import io.dingodb.sdk.common.serial.Buf;

/**
 * bool类型对应的序列化schema。
 */
public class BooleanSchema implements DingoSchema<Boolean> {

    /**
     * 此boolSchema对应的列在一行中的位置。
     */
    private int index;

    /**
     * 此schema是否用于key中。
     */
    private boolean isKey;

    /**
     * 是否允许为null，bool类型允许null值。
     */
    private boolean allowNull = true;

    /**
     * 略
     */
    public BooleanSchema() {
    }

    /**
     * 略
     */
    public BooleanSchema(int index) {
        this.index = index;
    }

    /**
     * 获得当前schema的类型。
     */
    @Override
    public Type getType() {
        return Type.BOOLEAN;
    }

    /**
     * 略
     */
    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    /**
     * 略
     */
    @Override
    public int getIndex() {
        return index;
    }

    /**
     * 略
     */
    @Override
    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    /**
     * 略
     */
    @Override
    public boolean isKey() {
        return isKey;
    }

    /**
     * 略
     */
    @Override
    public int getLength() {
        if (allowNull) {
            return getWithNullTagLength();
        }
        return getDataLength();
    }

    /**
     * 如果类型允许为null，那么长度需要包含标志位字节。
     * @return
     */
    private int getWithNullTagLength() {
        return 2;
    }

    /**
     * 一个bool类型占用一个字节。
     */
    private int getDataLength() {
        return 1;
    }

    /**
     * 略
     */
    @Override
    public void setAllowNull(boolean allowNull) {
        this.allowNull = allowNull;
    }

    /**
     * 略
     */
    @Override
    public boolean isAllowNull() {
        return allowNull;
    }

    /**
     * 对key编码.
     * @param buf 待补充.
     * @param data 待补充.
     */
    @Override
    public void encodeKey(Buf buf, Boolean data) {
        if (allowNull) {
            buf.ensureRemainder(getWithNullTagLength());
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeData(buf, data);
            }
        } else {
            buf.ensureRemainder(getDataLength());
            internalEncodeData(buf, data);
        }
    }

    /**
     * 对用于更新的key进行编码，更新key的编码不需要进行空间检测了。
     * @param buf 待补充.
     * @param data 待补充.
     */
    @Override
    public void encodeKeyForUpdate(Buf buf, Boolean data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeData(buf, data);
            }
        } else {
            internalEncodeData(buf, data);
        }
    }

    /**
     * null值编码.
     * @param buf
     */
    private void internalEncodeNull(Buf buf) {
        buf.write((byte) 0);
    }

    /**
     * bool值true,false编码.
     * @param buf
     * @param b
     */
    private void internalEncodeData(Buf buf, Boolean b) {
        if (b) {
            buf.write((byte) 1);
        } else {
            buf.write((byte) 0);
        }
    }

    /**
     * 对key解码。
     * @param buf 待补充.
     * @return
     */
    @Override
    public Boolean decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        return internalDecodeData(buf);
    }

    @Override
    public Boolean decodeKeyPrefix(Buf buf) {
        return decodeKey(buf);
    }

    private Boolean internalDecodeData(Buf buf) {
        return buf.read() == (byte) 0 ? false : true;
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skip(getLength());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Boolean data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Boolean data) {
        if (allowNull) {
            buf.ensureRemainder(getWithNullTagLength());
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeData(buf, data);
            }
        } else {
            buf.ensureRemainder(getDataLength());
            internalEncodeData(buf, data);
        }
    }

    @Override
    public Boolean decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        return internalDecodeData(buf);
    }

    @Override
    public void skipValue(Buf buf) {
        buf.skip(getLength());
    }
}
