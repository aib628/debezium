/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.parser;

import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.connector.kudu.CDCEventLog;

/**
 * source&sink的key.converter&value.converter须成对使用
 * 该AvroSinkRecordParser解析场景为：
 *
 * <pre>
 * source.key.converter&source.value.converter = io.confluent.connect.avro.AvroConverter
 * sink.key.converter&sink.value.converter = io.confluent.connect.avro.AvroConverter
 * </pre>
 */
public class AvroSinkRecordParser implements SinkRecordParser {

    @Override
    public CDCEventLog parse(SinkRecord sinkRecord) {
        CDCEventLog eventLog = new CDCEventLog();
        parseKey(sinkRecord, eventLog);
        parseValue(sinkRecord, eventLog);

        return eventLog;
    }

    private void parseKey(SinkRecord sinkRecord, CDCEventLog eventLog) {
        if (sinkRecord.keySchema().fields() == null) { // 顶层节点一定是Struct类型
            return;
        }

        for (Field schemaField : sinkRecord.keySchema().fields()) {
            CDCEventLog.NodeDefinition nodeDefinition = parseSimpleNode(schemaField, (Struct) sinkRecord.key());

            // nodeDefinition.className(JacksonUtils.getNodeValue(schema, KEY_NAME).orElse(null));
            eventLog.set(nodeDefinition.owner(CDCEventLog.DefinitionOwner.IDS));
        }

        eventLog.setKeySchemaVersion(sinkRecord.keySchema().version());
    }

    // sinkRecord.key() or sinkRecord.value()
    private String getNodeValue(Field fieldNode, Struct valueNode) {
        if (valueNode.get(fieldNode.name()) != null) {
            return valueNode.get(fieldNode.name()).toString();
        }

        if (fieldNode.schema().defaultValue() != null) {
            return fieldNode.schema().defaultValue().toString();
        }

        return null;
    }

    private void parseValue(SinkRecord sinkRecord, CDCEventLog eventLog) {
        if (sinkRecord.valueSchema().fields() == null) { // 顶层节点一定是Struct类型
            return;
        }

        for (Field schemaField : sinkRecord.valueSchema().fields()) {
            if (schemaField.schema().type() == Schema.Type.STRUCT) {
                parseStructNode(eventLog, schemaField, (Struct) sinkRecord.value());
            }
            else {
                eventLog.set(parseSimpleNode(schemaField, (Struct) sinkRecord.value()).setOwnerByName());
            }
        }

        eventLog.setValueSchemaVersion(sinkRecord.valueSchema().version());
    }

    private CDCEventLog.NodeDefinition parseSimpleNode(Field schemaField, Struct valueNode) {
        CDCEventLog.NodeDefinition nodeDefinition = CDCEventLog.NodeDefinition.builder();
        nodeDefinition.name(schemaField.name());
        nodeDefinition.optional(schemaField.schema().isOptional());
        nodeDefinition.type(schemaField.schema().type());
        nodeDefinition.value(getNodeValue(schemaField, valueNode));

        if (schemaField.schema().type() == Schema.Type.STRUCT) {
            List<Field> fields = schemaField.schema().fields();
        }

        return nodeDefinition;
    }

    private void parseStructNode(CDCEventLog eventLog, Field schemaField, Struct valueNode) {
        for (Field field : schemaField.schema().fields()) {
            if (valueNode.getStruct(schemaField.name()) == null) {
                continue;// 不存在对应节点，比如insert操作时的before节点是没有的
            }

            CDCEventLog.DefinitionOwner owner = CDCEventLog.DefinitionOwner.forCode(schemaField.name());
            eventLog.set(parseSimpleNode(field, valueNode.getStruct(schemaField.name())).owner(owner));
        }
    }

}
