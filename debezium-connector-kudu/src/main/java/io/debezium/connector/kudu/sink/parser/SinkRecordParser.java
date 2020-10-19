/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.parser;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.KuduConfig;

public interface SinkRecordParser {

    String KEY_NAME = "name";
    String KEY_FIELD = "field";
    String KEY_FIELDS = "fields";
    String KEY_OPTIONAL = "optional";
    String KEY_VERSION = "version";
    String AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
    Logger logger = LoggerFactory.getLogger(SinkRecordParser.class);

    /**
     * Parses the key and values based on the key and values schema.
     */
    CDCEventLog parse(SinkRecord sinkRecord) throws Exception;

    /**
     * Parses the key and values based on the key and values schema.
     * converter在sink中默认是可选配置，如果未配置则从Worker配置中获取。所以最好的方式是获取
     * org.apache.kafka.connect.runtime.WorkerSinkTask.keyConverter的值，但由于该值在context中是私有的，因此无法获取到。
     *
     * @see io.debezium.config.CommonConnectorConfig.isUsingAvroConverter
     * @see org.apache.kafka.connect.runtime.WorkerSinkTaskContext#sinkTask
     * @see org.apache.kafka.connect.runtime.WorkerSinkTask#keyConverter
     */
    static CDCEventLog parse(SinkRecord sinkRecord, KuduConfig config) throws Exception {
        Object converter = config.originals().get("key.converter"); // 根据序列化方式决定Parser解析器
        if (converter != null && AVRO_CONVERTER.equals(converter.toString())) {// 如果未配置，走了默认的Worker配置此处无法获取到，可能会导致不正确
            return new AvroSinkRecordParser().parse(sinkRecord);
        }

        return new JsonSinkRecordParser().parse(sinkRecord);
    }

    default Schema.Type mapType(String typeName) {
        List<String> schemaTypes = Arrays.stream(Schema.Type.values()).map(Schema.Type::name).collect(Collectors.toList());
        if (schemaTypes.contains(typeName)) {
            return Schema.Type.valueOf(typeName);
        }

        switch (typeName) {
            case "FLOAT":
                return Schema.Type.FLOAT32;
            case "DOUBLE":
                return Schema.Type.FLOAT64;
        }

        logger.warn("No type matched, return string default, {}", typeName);
        return Schema.Type.STRING; // No match, return string default.
    }

}
