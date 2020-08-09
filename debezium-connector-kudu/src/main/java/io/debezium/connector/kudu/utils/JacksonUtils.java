/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.utils;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;

import io.debezium.connector.kudu.sink.parser.AbsentNodeException;

public class JacksonUtils {

    public static JsonNode getMandatoryJsonNodes(JsonNode jsonNode, String... nodeNames) {
        JsonNode jsonNodeToUse = jsonNode;
        for (String nodeName : nodeNames) {
            jsonNodeToUse = getMandatoryJsonNode(jsonNodeToUse, nodeName);
        }

        return jsonNodeToUse;
    }

    public static JsonNode getMandatoryJsonNode(JsonNode jsonNode, String nodeName) {
        Objects.requireNonNull(jsonNode, "node must not be null or NullNode");
        return getJsonNode(jsonNode, nodeName).orElseThrow(AbsentNodeException.supplier(nodeName));
    }

    public static Optional<JsonNode> getJsonNode(JsonNode jsonNode, String nodeName) {
        if (jsonNode == null || jsonNode instanceof NullNode) {
            return Optional.empty();
        }

        JsonNode node = jsonNode.get(nodeName);
        if (node instanceof NullNode) {
            return Optional.empty();
        }

        return Optional.ofNullable(node);
    }

    public static Optional<String> getNodeValue(JsonNode jsonNode, String nodeName) {
        Optional<JsonNode> valueNode = getJsonNode(jsonNode, nodeName);
        return valueNode.map(JsonNode::asText);
    }

    public static String getMandatoryNodeValue(JsonNode jsonNode, String nodeName) {
        Optional<String> nodeValue = getNodeValue(jsonNode, nodeName);
        return nodeValue.orElseThrow(AbsentNodeException.supplier(nodeName));
    }

    public static Optional<Boolean> getBooleanNodeValue(JsonNode jsonNode, String nodeName) {
        Optional<JsonNode> valueNode = getJsonNode(jsonNode, nodeName);
        return valueNode.map(JsonNode::asBoolean);
    }

}
