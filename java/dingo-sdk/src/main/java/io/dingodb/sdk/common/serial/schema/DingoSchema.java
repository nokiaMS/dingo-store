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
 * dingoSchema是用于序列化的一个类，每个数据类型都对应一个DingoSchema的具体实现.
 * 序列化过程中的类型信息只与dingoSchema有关系，在序列化之前，会把类型转换成对应的schema，一旦进入到序列化过程中，就与原类型没有关系了。
 * @param <T>
 */
public interface DingoSchema<T> {

    /**
     * null值定义.
     */
    byte NULL = 0;

    /**
     * notnull定义.
     */
    byte NOTNULL = 1;

    /**
     * 获得当前schema对象的类型.
     * @return 待补充.
     */
    Type getType();

    /**
     * 设置schema对应的在tuple中的位置信息.
     * @param index 待补充.
     */
    void setIndex(int index);

    /**
     * 获得schema在tuple中的位置信息.
     * @return 待补充.
     */
    int getIndex();

    /**
     * 设置此schema是否为key的标志位.
     * @param isKey
     */
    void setIsKey(boolean isKey);

    /**
     * 获得此schema是否为key的标志位.
     * @return 待补充.
     */
    boolean isKey();

    /**
     * 获得类型占用的字节长度.
     * @return 待补充.
     */
    int getLength();

    /**
     * 设置类型是否允许为null.
     * @param allowNull 待补充.
     */
    void setAllowNull(boolean allowNull);

    /**
     * 判断类型是否允许为null.
     * @return 待补充.
     */
    boolean isAllowNull();

    /**
     * 对key进行编码.
     * @param buf 待补充.
     * @param data 待补充.
     */
    void encodeKey(Buf buf, T data);

    /**
     * 对key编码用于更新字段.
     * @param buf 待补充.
     * @param data 待补充.
     */
    void encodeKeyForUpdate(Buf buf, T data);

    /**
     * 对key进行解码.
     * @param buf 待补充.
     * @return 待补充.
     */
    T decodeKey(Buf buf);

    T decodeKeyPrefix(Buf buf);

    /**
     * 跳过key的长度个字节.
     * @param buf 待补充.
     */
    void skipKey(Buf buf);

    void encodeKeyPrefix(Buf buf, T data);

    /**
     * 对值进行编码.
     * @param buf 待补充.
     * @param data 待补充.
     */
    void encodeValue(Buf buf, T data);

    /**
     * 对值进行解码.
     * @param buf 待补充.
     * @return 待补充.
     */
    T decodeValue(Buf buf);

    /**
     * 跳过值的长度个字节.
     * @param buf 待补充.
     */
    void skipValue(Buf buf);

}
