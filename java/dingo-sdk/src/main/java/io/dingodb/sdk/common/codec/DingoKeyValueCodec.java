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

package io.dingodb.sdk.common.codec;

import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.serial.RecordDecoder;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

/**
 * kv序列化.
 */
public class DingoKeyValueCodec implements KeyValueCodec {

    /**
     * table id. 表id.
     */
    private final long id;

    /**
     * table元信息.
     */
    private final List<DingoSchema> schemas;

    /**
     * record encoder.  record编码器.
     */
    RecordEncoder re;

    /**
     * record decoder. record解码器.
     */
    RecordDecoder rd;

    /**
     * 构造函数.
     * @param id    表id.
     * @param schemas   表的元信息.
     */
    public DingoKeyValueCodec(long id, List<DingoSchema> schemas) {
        //如果没有指定元信息版本号则默认为1.
        this(1, id, schemas);
    }

    /**
     * 构造函数.
     * @param schemaVersion     元信息版本号.
     * @param id                表id.
     * @param schemas           表元信息.
     */
    public DingoKeyValueCodec(int schemaVersion, long id, List<DingoSchema> schemas) {
        //设置表元信息.
        this.schemas = schemas;
        //设置表id.
        this.id = id;

        //设置record编码器.
        re = new RecordEncoder(schemaVersion, schemas, id);
        //设置record解码器.
        rd = new RecordDecoder(schemaVersion, schemas, id);
    }

    /**
     * 定义of函数.
     * @param table
     * @return
     */
    public static DingoKeyValueCodec of(Table table) {
        return of(
                table.getVersion(),
                Optional.mapOrGet(table.id(), DingoCommonId::entityId, () -> 0L),
                table.getColumns()
        );
    }

    public static DingoKeyValueCodec of(long id, Table table) {
        return of(table.getVersion(), id, table);
    }

    public static DingoKeyValueCodec of(long id, List<Column> columns) {
        return of(1, id, columns);
    }

    public static DingoKeyValueCodec of(int schemaVersion,long id, Table table) {
        return of(schemaVersion, id, table.getColumns());
    }

    public static DingoKeyValueCodec of(int schemaVersion, long id, List<Column> columns) {
        return new DingoKeyValueCodec(schemaVersion, id, CodecUtils.createSchemaForColumns(columns));
    }

    @Override
    public Object[] decode(KeyValue keyValue) {
        return rd.decode(keyValue);
    }

    @Override
    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        return rd.decodeKeyPrefix(keyPrefix);
    }

    /**
     * 对value进行编码.
     * @param record    待补充.
     * @return      待补充.
     */
    @Override
    public KeyValue encode(Object @NonNull [] record) {
        return re.encode(record);
    }

    @Override
    public byte[] encodeKey(Object[] record) {
        return re.encodeKey(record);
    }

    @Override
    public byte[] encodeKeyPrefix(Object[] record, int columnCount) {
        return re.encodeKeyPrefix(record, columnCount);
    }

    @Override
    public byte[] encodeMinKeyPrefix() {
        return re.encodeMinKeyPrefix();
    }

    @Override
    public byte[] encodeMaxKeyPrefix() {
        return re.encodeMaxKeyPrefix();
    }

    public byte[] resetPrefix(byte[] key) {
        return re.resetKeyPrefix(key, id);
    }

    @Override
    public byte[] resetPrefix(byte[] key, long prefix) {
        return re.resetKeyPrefix(key, prefix);
    }
}
