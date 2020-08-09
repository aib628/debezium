/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.kudu.sink.parser.AbsentNodeException;
import io.debezium.data.Envelope;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class CDCEventLog {

    // regroup to speed up
    private final Map<String, NodeDefinition> namesByDefinition = new HashMap<>();

    // definition
    private final List<NodeDefinition> ids = new ArrayList<>(); // primaryKey definition, parse from key
    private final List<NodeDefinition> before = new ArrayList<>();
    private final List<NodeDefinition> after = new ArrayList<>();
    private final List<NodeDefinition> source = new ArrayList<>();
    private final List<NodeDefinition> transaction = new ArrayList<>();
    private Envelope.Operation op = Envelope.Operation.CREATE;
    private Long tsInMillis = 0L;

    public NodeDefinition getNodeDefinitionMandatory(DefinitionOwner owner, String nodeName) {
        return getNodeDefinition(owner, nodeName).orElseThrow(AbsentNodeException.supplier(owner.group() + nodeName));
    }

    public Optional<NodeDefinition> getNodeDefinition(DefinitionOwner owner, String nodeName) {
        return Optional.ofNullable(namesByDefinition.get(owner.group() + nodeName));
    }

    public NodeDefinition getSourceNodeDefinitionMandatory(String nodeName) {
        return getNodeDefinitionMandatory(DefinitionOwner.SOURCE, nodeName);
    }

    public String getConnectType() {
        return getSourceNodeDefinitionMandatory(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY).getNodeValue();
    }

    public String getTableName() {
        return getSourceNodeDefinitionMandatory(AbstractSourceInfo.TABLE_NAME_KEY).getNodeValue();
    }

    public String getDatabaseName() {
        return getSourceNodeDefinitionMandatory(AbstractSourceInfo.DATABASE_NAME_KEY).getNodeValue();
    }

    public String getCanonicalTableName() {
        return getDatabaseName() + "." + getTableName();
    }

    public void addId(NodeDefinition id) {
        id.addToMap(namesByDefinition, DefinitionOwner.IDS.group());
        this.ids.add(id);
    }

    public void addBefore(NodeDefinition nodeDefinition) {
        nodeDefinition.addToMap(namesByDefinition, DefinitionOwner.BEFORE.group());
        before.add(nodeDefinition);
    }

    public void addAfter(NodeDefinition nodeDefinition) {
        nodeDefinition.addToMap(namesByDefinition, DefinitionOwner.AFTER.group());
        after.add(nodeDefinition);
    }

    public void addSource(NodeDefinition nodeDefinition) {
        nodeDefinition.addToMap(namesByDefinition, DefinitionOwner.SOURCE.group());
        source.add(nodeDefinition);
    }

    public void addTransaction(NodeDefinition nodeDefinition) {
        nodeDefinition.addToMap(namesByDefinition, DefinitionOwner.TRANSACTION.group());
        transaction.add(nodeDefinition);
    }

    public void setOp(Envelope.Operation op) {
        this.op = op;
    }

    public void setTsInMillis(Long tsInMillis) {
        this.tsInMillis = tsInMillis;
    }

    public void set(NodeDefinition definition) {
        if (definition.owner == null) {
            log.warn("find undefined filed : {}", definition.nodeName);
            return;
        }

        switch (definition.owner) {
            case IDS:
                addId(definition);
                break;
            case BEFORE:
                addBefore(definition);
                break;
            case AFTER:
                addAfter(definition);
                break;
            case SOURCE:
                addSource(definition);
                break;
            case TRANSACTION:
                addTransaction(definition);
                break;
            case OP:
                setOp(Envelope.Operation.forCode(definition.nodeValue));
                break;
            case TS:
                setTsInMillis(Long.parseLong(definition.nodeValue));
                break;
        }

        definition.addToMap(namesByDefinition, definition.owner.group());
    }

    @Override
    public String toString() {
        return new StringBuilder("CDCEventLog(")
                .append("ids=").append(ids).append(",")
                .append("before=").append(before).append(",")
                .append("after=").append(after).append(",")
                .append("source=").append(source).append(",")
                .append("transaction=").append(transaction).append(",")
                .append("op=").append(op).append(",")
                .append("ts=").append(tsInMillis)
                .append(")")
                .toString();
    }

    @Getter
    public static class NodeDefinition {
        private DefinitionOwner owner;
        private String nodeName;
        private String nodeValue;
        private Schema.Type nodeType;
        private boolean optional;
        private String className; // when mysql type is Time:Enum:Geometry, this value provided.

        public void addToMap(Map<String, NodeDefinition> namesByDefinition, String groupPrefix) {
            namesByDefinition.put(groupPrefix + nodeName, this);
        }

        public static NodeDefinition builder() {
            return new NodeDefinition();
        }

        public NodeDefinition owner(DefinitionOwner owner) {
            this.owner = owner;
            return this;
        }

        public NodeDefinition setOwnerByName() {
            Objects.requireNonNull(this.nodeName, "nodeName must not be null");
            this.owner = DefinitionOwner.forCode(this.nodeName.toUpperCase());
            return this;
        }

        public NodeDefinition name(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public NodeDefinition value(String nodeValue) {
            this.nodeValue = nodeValue;
            return this;
        }

        public NodeDefinition type(Schema.Type type) {
            this.nodeType = type;
            return this;
        }

        public NodeDefinition optional(boolean optional) {
            this.optional = optional;
            return this;
        }

        public NodeDefinition className(String className) {
            this.className = className;
            return this;
        }

        @Override
        public String toString() {
            return new StringBuilder("NodeDefinition(")
                    .append("owner=").append(owner).append(",")
                    .append("nodeName=").append(nodeName).append(",")
                    .append("nodeValue=").append(nodeValue).append(",")
                    .append("nodeType=").append(nodeType).append(",")
                    .append("optional=").append(optional).append(",")
                    .append("className=").append(className)
                    .append(")")
                    .toString();
        }
    }

    public enum DefinitionOwner {
        IDS("ids"),

        BEFORE(Envelope.FieldName.BEFORE),

        AFTER(Envelope.FieldName.AFTER),

        SOURCE(Envelope.FieldName.SOURCE),

        OP(Envelope.FieldName.OPERATION),

        TS(Envelope.FieldName.TIMESTAMP),

        TRANSACTION(Envelope.FieldName.TRANSACTION);

        @Getter
        private final String code;

        public String group() {
            return this.code + ".";
        }

        DefinitionOwner(String code) {
            this.code = code;
        }

        public static DefinitionOwner forCode(String code) {
            Objects.requireNonNull(code, "code must not be null");
            for (DefinitionOwner owner : values()) {
                if (owner.code.equalsIgnoreCase(code)) {
                    return owner;
                }
            }

            return null;
        }
    }

}
