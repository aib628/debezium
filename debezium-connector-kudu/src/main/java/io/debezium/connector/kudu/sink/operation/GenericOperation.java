/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.operation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.KuduConfig;
import io.debezium.connector.kudu.utils.KuduUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class GenericOperation extends AtomicReference<Operation> {

    public boolean needFlush = false; // 是否需要Flush,以保证DDL与DML顺序
    protected final KuduConfig config;
    protected final CDCEventLog eventLog;

    // cache to speed up
    protected static final AtomicInteger keySchemaVersion = new AtomicInteger(0);
    protected static final AtomicInteger valueSchemaVersion = new AtomicInteger(0);
    protected static final Map<String, KuduTable> kuduTables = new ConcurrentHashMap<>();
    protected static final Map<String, TableColumns> tablesDefinitions = new ConcurrentHashMap<>();

    public GenericOperation(CDCEventLog eventLog, KuduConfig config) {
        this.eventLog = eventLog;
        this.config = config;

        String tableName = config.getMappedTable(eventLog.getCanonicalTableName());
        if (tablesDefinitions.isEmpty() || !tablesDefinitions.containsKey(tableName)) {
            initTableMetadata(); // Connector重启也会导致init，小概率存在碰上DDL操作，暂时忽略这种情况
            log.info("Init tablesDefinitions use ids:{}", eventLog.getIds().stream().map(CDCEventLog.NodeDefinition::getNodeValue).collect(Collectors.toList()));
        }
    }

    public abstract Operation operate(KuduClient client) throws KuduException;

    public static void clearKuduTableDefinitions() {
        kuduTables.clear(); // 重启Connector会重新获取KuduClient，会导致：Applied operations must be created from a KuduTable instance opened from the same client that opened this KuduSession
    }

    public static GenericOperation operate(CDCEventLog eventLog, KuduClient client, KuduConfig config) throws KuduException {
        long beginTime = System.currentTimeMillis();
        GenericOperation genericOperation = create(eventLog, config);
        genericOperation.set(genericOperation.operate(client));
        if (KuduUtils.debugLogOpened(config)) {
            log.info("convert to operation costs : {} ms", System.currentTimeMillis() - beginTime);
        }

        return genericOperation;
    }

    private static GenericOperation create(CDCEventLog eventLog, KuduConfig config) {
        if ("mysql".equals(eventLog.getConnectType())) {
            return new MysqlSourceOperation(eventLog, config);
        }

        throw new DataException("Unsupported connector type : " + eventLog.getConnectType());
    }

    private void initTableMetadata() {
        String mappedTable = config.getMappedTable(eventLog.getCanonicalTableName());
        TableColumns original = tablesDefinitions.computeIfAbsent(mappedTable, (tableName) -> {
            TableColumns tableColumns = new TableColumns(mappedTable);
            tableColumns.keys(eventLog.getIds());

            // occur when reset connector but first record is delete
            if (!eventLog.getAfter().isEmpty()) {
                tableColumns.columns(eventLog.getAfter());
            }

            return tableColumns;
        });

        // make up columns when first record have not after node case
        if (original.getColumns().isEmpty() && !eventLog.getAfter().isEmpty()) {
            log.warn("Init tables definitions, but after node is empty, {}", mappedTable);
            original.columns(eventLog.getAfter());
        }

        updateSchemaVersionByEventLog();
    }

    // KUDU不支持改字段类型，暂不做检测，如遇改字段类型由KUDU直接报错!!!
    protected Optional<AlterTableOptions> updateTableMetadata(String tableName) {
        if (eventLog.getAfter().isEmpty()) {
            return Optional.empty(); // 只有delete操作时，After节点数据才是空的，此种场景不需要判断Schema是否变化
        }

        // AVRO格式可通过SchemaVersion判断表结构是否有变化，AVRO格式SchemaVersion最低版本为1，可通过该字段判断是否是AVRO格式
        if (!eventLog.needUpdateTableMetadataDefinition(keySchemaVersion.get(), valueSchemaVersion.get())) {
            return Optional.empty();
        }

        TableColumns original = tablesDefinitions.get(tableName);
        if (original == null) { // 能到达update方法，一定会经过init初始化，original为空理论不存在这种情况
            return Optional.empty();
        }

        // 新增主键列Keys
        List<CDCEventLog.NodeDefinition> newKeys = eventLog.getIds().stream().filter((it) -> {
            return !original.keys.containsKey(it.getNodeName());
        }).collect(Collectors.toList());

        // 删除主键列Keys
        List<CDCEventLog.NodeDefinition> deletedKeys = original.keys.values().stream().filter((it) -> {
            return !eventLog.getNodeDefinition(CDCEventLog.DefinitionOwner.IDS, it.getNodeName()).isPresent();
        }).collect(Collectors.toList());

        // 新增普通列Columns
        List<CDCEventLog.NodeDefinition> newColumns = eventLog.getAfter().stream().filter((it) -> {
            return !original.keys.containsKey(it.getNodeName()) && !original.columns.containsKey(it.getNodeName());
        }).collect(Collectors.toList());

        // 删除普通列
        List<CDCEventLog.NodeDefinition> deletedColumns = original.columns.values().stream().filter((it) -> {
            return !eventLog.getNodeDefinition(CDCEventLog.DefinitionOwner.AFTER, it.getNodeName()).isPresent();
        }).collect(Collectors.toList());

        deletedKeys.forEach((deletedKey) -> {
            log.info("Find deleted primary key column : {}.{}", original.tableName, deletedKey.getNodeName());
            original.keys.remove(deletedKey.getNodeName());
        });

        deletedColumns.forEach((deleteColumn) -> {
            log.info("Find deleted column : {}.{}", original.tableName, deleteColumn.getNodeName());
            original.columns.remove(deleteColumn.getNodeName());
        });

        // 更新表字段最新状态至缓存
        newKeys.forEach((newKey) -> {
            log.info("Find new primary key column : {}.{}", original.tableName, newKey.getNodeName());
            original.keys.put(newKey.getNodeName(), newKey);
        });

        newColumns.forEach((newColumn) -> {
            log.info("Find new column : {}.{}", original.tableName, newColumn.getNodeName());
            original.columns.put(newColumn.getNodeName(), newColumn);
        });

        if (newKeys.size() > 0 || deletedKeys.size() > 0 || newColumns.size() > 0 || deletedColumns.size() > 0) {
            AlterTableOptions options = new AlterTableOptions();

            deletedKeys.forEach((it) -> {
                options.dropColumn(it.getNodeName());
            });

            deletedColumns.forEach((it) -> {
                options.dropColumn(it.getNodeName());
            });

            newKeys.forEach((it) -> {
                options.addColumn(new ColumnSchema.ColumnSchemaBuilder(it.getNodeName(), mapType(it.getNodeType())).key(true).nullable(it.isOptional()).build());
            });

            newColumns.forEach((it) -> {
                options.addColumn(new ColumnSchema.ColumnSchemaBuilder(it.getNodeName(), mapType(it.getNodeType())).key(false).nullable(it.isOptional()).build());
            });

            return Optional.of(options);
        }

        return Optional.empty();
    }

    protected void updateSchemaVersionByEventLog() {
        if (eventLog.getKeySchemaVersion() != null) {
            keySchemaVersion.set(eventLog.getKeySchemaVersion());
        }

        if (eventLog.getValueSchemaVersion() != null) {
            valueSchemaVersion.set(eventLog.getValueSchemaVersion());
        }
    }

    protected Type mapType(Schema.Type nodeType) {
        List<String> typeNames = Arrays.stream(Type.values()).map(Type::name).collect(Collectors.toList());
        if (typeNames.contains(nodeType.name())) {
            return Type.valueOf(nodeType.name());
        }

        switch (nodeType) {
            case INT8:
                return Type.INT8;
            case INT16:
                return Type.INT16;
            case INT32:
                return Type.INT32;
            case INT64:
                return Type.INT64;
            case FLOAT32:
                return Type.FLOAT;
            case FLOAT64:
                return Type.DOUBLE;
            case BOOLEAN:
                return Type.BOOL;
            case STRING:
                return Type.STRING;
            case BYTES:
                return Type.BINARY;
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("Not supported type:" + nodeType.name());
        }
    }

    @Data
    public class TableColumns {
        private String tableName;
        private final Map<String, CDCEventLog.NodeDefinition> keys = new HashMap<>();
        private final Map<String, CDCEventLog.NodeDefinition> columns = new HashMap<>();

        TableColumns(String tableName) {
            this.tableName = tableName;
        }

        public TableColumns keys(List<CDCEventLog.NodeDefinition> ids) {
            ids.forEach((id) -> {
                keys.put(id.getNodeName(), id);
            });

            return this;
        }

        public TableColumns columns(List<CDCEventLog.NodeDefinition> columns) {
            columns.forEach((column) -> {
                this.columns.put(column.getNodeName(), column);
            });

            return this;
        }

        public Map<String, CDCEventLog.NodeDefinition> getColumnsWithoutKeys() {
            Map<String, CDCEventLog.NodeDefinition> columnsWithoutKeys = new HashMap<>();
            for (Map.Entry<String, CDCEventLog.NodeDefinition> entry : columns.entrySet()) {
                if (keys.containsKey(entry.getKey())) {
                    continue;
                }

                columnsWithoutKeys.put(entry.getKey(), entry.getValue());
            }

            return columnsWithoutKeys;
        }
    }

}
