/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.operation;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.KuduConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MysqlSourceOperation extends GenericOperation {

    public MysqlSourceOperation(CDCEventLog eventLog, KuduConfig config) {
        super(eventLog, config);
    }

    @Override
    public Operation operate(KuduClient client) throws KuduException {
        String tableName = config.getMappedTable(eventLog.getCanonicalTableName());
        if (!client.tableExists(tableName)) {
            log.info("Table not exist, begin to create, {}", tableName);
            createTable(client, tableName);
        }
        else if (tablesDefinitions.containsKey(tableName)) {
            Optional<AlterTableOptions> options = updateTableMetadata(tableName);
            if (options.isPresent()) {
                log.info("Table schema changed, applying ...");
                client.alterTable(tableName, options.get());
                this.needFlush = true; // Flush to db.
            }
        }

        return createOperation(client, tableName);
    }

    private Operation createOperation(KuduClient client, String tableName) throws KuduException {
        KuduTable kuduTable = client.openTable(tableName);
        switch (eventLog.getOp()) {
            case CREATE:
                return createInsertOrUpsertOperation(kuduTable);
            case UPDATE:
                return createUpsertOperation(kuduTable);
            case DELETE:
                return createDeleteOperation(kuduTable);
            default:
                throw new RuntimeException("Unsupported operation, c、u、d only, but " + eventLog.getOp());
        }
    }

    private Operation createInsertOrUpsertOperation(KuduTable kuduTable) {
        String insertMode = config.getString(KuduConfig.KEY_MASTER_ADDRESSES);
        if (KuduConfig.InsertMode.INSERT == KuduConfig.InsertMode.parse(insertMode)) {
            return createInsertOperation(kuduTable);
        }
        else {
            return createUpsertOperation(kuduTable);
        }
    }

    private Operation createInsertOperation(KuduTable kuduTable) {
        log.info("Creating insert operation, {}:{}", kuduTable.getName(), transformToLog(eventLog.getIds()));
        Operation operation = kuduTable.newInsert();
        PartialRow row = operation.getRow();

        // create or update by after node
        eventLog.getAfter().forEach((key) -> fillRow(row, key));

        return operation;
    }

    private Operation createUpsertOperation(KuduTable kuduTable) {
        log.info("Creating upsert operation, {}:{}", kuduTable.getName(), transformToLog(eventLog.getIds()));
        Operation operation = kuduTable.newUpsert();
        PartialRow row = operation.getRow();

        // create or update by after node
        eventLog.getAfter().forEach((key) -> fillRow(row, key));

        return operation;
    }

    private Operation createDeleteOperation(KuduTable kuduTable) {
        log.info("Creating delete operation, {}:{}", kuduTable.getName(), transformToLog(eventLog.getIds()));
        Operation operation = kuduTable.newDelete();
        PartialRow row = operation.getRow();

        // delete by primary key
        eventLog.getIds().forEach((key) -> fillRow(row, key));

        return operation;
    }

    private void createTable(KuduClient client, String tableName) {
        GenericOperation.TableColumns tableColumns = tablesDefinitions.get(tableName);
        List<ColumnSchema> columns = new ArrayList<>(tableColumns.getColumns().size());

        // 添加列，注意主键列必须放在最前面
        for (CDCEventLog.NodeDefinition column : tableColumns.getKeys().values()) {
            Type columnType = mapType(column.getNodeType());
            columns.add(new ColumnSchema.ColumnSchemaBuilder(column.getNodeName(), columnType).key(true).nullable(column.isOptional()).build());
        }

        tableColumns.getColumnsWithoutKeys().values().forEach((column) -> {
            Type columnType = mapType(column.getNodeType());
            columns.add(new ColumnSchema.ColumnSchemaBuilder(column.getNodeName(), columnType).key(false).nullable(column.isOptional()).build());
        });

        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.addHashPartitions(Lists.newArrayList(tableColumns.getKeys().keySet()), 3);

        try {
            client.createTable(tableName, new Schema(columns), createTableOptions);
            log.info("Create table automatically succeed, tableName={},columns={}", tableName, columns);
        }
        catch (KuduException e) {
            String pattern = "Create table automatically failed, tableName=%s,columns=%s, for reason : %s";
            log.error(String.format(pattern, tableName, columns, e.getMessage()), e);
            throw new RuntimeException(String.format(pattern, tableName, columns, e.getMessage()), e);
        }
    }

    private void fillRow(PartialRow row, CDCEventLog.NodeDefinition definition) {
        if (definition.getNodeValue() == null) {
            row.setNull(definition.getNodeName());
            return;
        }

        switch (definition.getNodeType()) {
            case INT8:
                row.addByte(definition.getNodeName(), Byte.parseByte(definition.getNodeValue()));
                break;
            case INT16:
                row.addShort(definition.getNodeName(), Short.parseShort(definition.getNodeValue()));
                break;
            case INT32:
                row.addInt(definition.getNodeName(), Integer.parseInt(definition.getNodeValue()));
                break;
            case INT64:
                row.addLong(definition.getNodeName(), Long.parseLong(definition.getNodeValue()));
                break;
            case FLOAT32:
                row.addFloat(definition.getNodeName(), Float.parseFloat(definition.getNodeValue()));
                break;
            case FLOAT64:
                row.addDouble(definition.getNodeName(), Double.parseDouble(definition.getNodeValue()));
                break;
            case BOOLEAN:
                row.addBoolean(definition.getNodeName(), Boolean.parseBoolean(definition.getNodeValue()));
                break;
            case STRING:
                row.addString(definition.getNodeName(), definition.getNodeValue());
                break;
            case BYTES:
                row.addBinary(definition.getNodeName(), Base64.getDecoder().decode(definition.getNodeValue()));
                break;
            default:
                throw new RuntimeException("Unsupported column type:" + definition.getNodeType());
        }
    }

    private String transformToLog(List<CDCEventLog.NodeDefinition> definitions) {
        return definitions.stream().map((it) -> it.getNodeName() + ":" + it.getNodeValue()).collect(Collectors.joining(","));
    }
}
