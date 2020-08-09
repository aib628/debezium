/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.utils.JacksonUtils;
import io.debezium.converters.CloudEventsMaker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonSinkRecordParser implements SinkRecordParser {

    private static final JsonConverter keyJsonConverter = new JsonConverter();
    private static final JsonConverter valueJsonConverter = new JsonConverter();
    private static final JsonDeserializer keyJsonDeserializer = new JsonDeserializer();
    private static final JsonDeserializer valueJsonDeserializer = new JsonDeserializer();

    static {
        Map<String, Object> jsonConfig = new HashMap<>();
        jsonConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, JsonConverterConfig.SCHEMAS_ENABLE_DEFAULT);
        jsonConfig.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, JsonConverterConfig.SCHEMAS_CACHE_SIZE_DEFAULT);
        // jsonConfig.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());

        keyJsonConverter.configure(jsonConfig, true);
        valueJsonConverter.configure(jsonConfig, false);

        keyJsonDeserializer.configure(jsonConfig, true);
        valueJsonDeserializer.configure(jsonConfig, false);
    }

    @Override
    public CDCEventLog parse(SinkRecord sinkRecord) throws JsonProcessingException {
        CDCEventLog eventLog = new CDCEventLog();
        parseKey(sinkRecord, eventLog);
        parseValue(sinkRecord, eventLog);

        return eventLog;
    }

    private void parseKey(SinkRecord sinkRecord, CDCEventLog eventLog) throws JsonProcessingException {
        JsonNode jsonNode = parsePayload(sinkRecord, true);
        log.debug("key : {}", jsonNode.toPrettyString());

        JsonNode payload = JacksonUtils.getMandatoryJsonNode(jsonNode, CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME);
        for (JsonNode node : JacksonUtils.getMandatoryJsonNodes(jsonNode, CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME, KEY_FIELDS)) {
            eventLog.set(parseSimplePayloadBySchema(node, payload).owner(CDCEventLog.DefinitionOwner.IDS));
        }
    }

    private void parseValue(SinkRecord sinkRecord, CDCEventLog eventLog) throws JsonProcessingException {
        JsonNode jsonNode = parsePayload(sinkRecord, false);
        log.debug("values : {}", jsonNode.toPrettyString());

        JsonNode payload = JacksonUtils.getMandatoryJsonNode(jsonNode, CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME);
        for (JsonNode node : JacksonUtils.getMandatoryJsonNodes(jsonNode, CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME, KEY_FIELDS)) {
            String schemaType = JacksonUtils.getMandatoryNodeValue(node, CloudEventsMaker.FieldName.TYPE);
            if (Schema.Type.valueOf(schemaType.toUpperCase()) == Schema.Type.STRUCT) {
                String nodeName = JacksonUtils.getMandatoryNodeValue(node, KEY_FIELD);
                Optional<JsonNode> nodePayload = JacksonUtils.getJsonNode(payload, nodeName);
                if (!nodePayload.isPresent()) {
                    continue;
                }

                CDCEventLog.DefinitionOwner owner = CDCEventLog.DefinitionOwner.forCode(JacksonUtils.getMandatoryNodeValue(node, KEY_FIELD));
                parseStructPayloadBySchema(node, nodePayload.get(), eventLog, owner);
            }
            else {
                eventLog.set(parseSimplePayloadBySchema(node, payload).setOwnerByName());
            }
        }
    }

    // 解析传入schema与payload当前层的NodeDefinition,传入Schema.Type不为Struct类型
    private CDCEventLog.NodeDefinition parseSimplePayloadBySchema(JsonNode schema, JsonNode payload) {
        CDCEventLog.NodeDefinition nodeDefinition = CDCEventLog.NodeDefinition.builder();
        nodeDefinition.name(JacksonUtils.getNodeValue(schema, KEY_FIELD).orElse(null));
        nodeDefinition.optional(JacksonUtils.getBooleanNodeValue(schema, KEY_OPTIONAL).orElse(false));

        String schemaType = JacksonUtils.getMandatoryNodeValue(schema, CloudEventsMaker.FieldName.TYPE);
        nodeDefinition.type(mapType(schemaType.toUpperCase()));

        nodeDefinition.className(JacksonUtils.getNodeValue(schema, KEY_NAME).orElse(null));
        nodeDefinition.value(JacksonUtils.getNodeValue(payload, nodeDefinition.getNodeName()).orElse(null));
        return nodeDefinition;
    }

    // 解析传入schema与payload当前层的NodeDefinition,传入Schema.Type是Struct类型
    private void parseStructPayloadBySchema(JsonNode schema, JsonNode nodePayload, CDCEventLog eventLog, CDCEventLog.DefinitionOwner owner) {
        String nodeName = JacksonUtils.getNodeValue(schema, KEY_FIELD).orElseThrow(AbsentNodeException.supplier(KEY_FIELD));

        for (JsonNode nodeSchema : JacksonUtils.getMandatoryJsonNode(schema, KEY_FIELDS)) {
            eventLog.set(parseSimplePayloadBySchema(nodeSchema, nodePayload).owner(owner));
        }
    }

    private JsonNode parsePayload(SinkRecord record, boolean isKey) throws JsonProcessingException {
        String payloadNodeName = CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME;
        if (isKey) {
            byte[] bytes = keyJsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            JsonNode jsonNode = keyJsonDeserializer.deserialize(record.topic(), bytes);
            String payload = JacksonUtils.getNodeValue(jsonNode, payloadNodeName).orElseThrow(AbsentNodeException.supplier(payloadNodeName));
            return new ObjectMapper().readTree(payload);
        }
        else {
            byte[] bytes = valueJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            JsonNode jsonNode = valueJsonDeserializer.deserialize(record.topic(), bytes);
            String payload = JacksonUtils.getNodeValue(jsonNode, payloadNodeName).orElseThrow(AbsentNodeException.supplier(payloadNodeName));
            return new ObjectMapper().readTree(payload);
        }
    }

    private JsonNode getPayloadNode(JsonNode jsonNode) {
        String nodeName = CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME;
        return JacksonUtils.getJsonNode(jsonNode, nodeName).orElseThrow(AbsentNodeException.supplier(nodeName));
    }

    public Schema.Type mapType(String typeName) {
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

        log.warn("No type matched, return string default, {}", typeName);
        return Schema.Type.STRING; // No match, return string default.
    }
}
