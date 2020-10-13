/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu;

import io.debezium.config.EnumeratedValue;
import io.debezium.connector.kudu.utils.StringUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kudu.client.AsyncKuduClient;

@Slf4j
@Getter
@Setter
public abstract class KuduConfig extends AbstractConfig {

    /**
     * master节点地址:端口，多个以,隔开
     */
    public final static String KEY_MASTER_ADDRESSES = "kudu.masters";

    /**
     * 业务表名:[Kudu表名]映射
     */
    public final static String KEY_TABLE_WHITELIST = "tables";

    /**
     * 将所有Insert操作以Upsert模式写入，以支持幂等性
     */
    public final static String KEY_INSERT_MODE = "insert.mode";

    /**
     * 连接scan token的超时时间，如果不设置，则与operationTimeout一致
     */
    public final static String KEY_QUERY_TIMEOUT = "query.timeout";

    /**
     * 设置普通操作超时时间，默认30S
     */
    public final static String KEY_OPERATION_TIMEOUT = "operation.timeout";

    /**
     * 设置管理员操作(建表，删表)超时时间，默认30S
     */
    public final static String KEY_ADMIN_OPERATION_TIMEOUT = "admin.operation.timeout";

    /**
     * writer写入时session刷新模式
     * auto_flush_sync（默认）
     * auto_flush_background
     * manual_flush
     */
    public final static String KEY_FLUSH_MODE = "flush.mode";

    /**
     * The number of operations that can be buffered
     */
    public final static String KEY_BATCH_SIZE = "batch.size";

    private final Map<String, String> tableMap = new HashMap<>();

    public KuduConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals);
        analysisTableMap();
    }

    public static ConfigKeys commonConfigDefinition() {
        ConfigKeys keys = new ConfigKeys();
        keys.define(KEY_MASTER_ADDRESSES).type(ConfigDef.Type.STRING)
                .defaultValue(ConfigDef.NO_DEFAULT_VALUE).importance(ConfigDef.Importance.HIGH)
                .width(ConfigDef.Width.LONG).group("Connection")
                .documentation("The master address to use to open connection, cluster addresses split by comma.")
                .displayName("Kudu master address");

        keys.define(KEY_TABLE_WHITELIST).type(ConfigDef.Type.STRING)
                .defaultValue(ConfigDef.NO_DEFAULT_VALUE).importance(ConfigDef.Importance.HIGH)
                .width(ConfigDef.Width.LONG).group("Sink")
                .documentation(
                        "The kudu tables to sync, support source:sink table map, eg:source table name is table_mysql, when config table_mysql:table_kudu, the result table name in kudu is table_kudu")
                .displayName("Kudu table names");

        keys.define(KEY_INSERT_MODE, ConfigDef.Type.STRING)
                .defaultValue(InsertMode.UPSERT.name()).importance(ConfigDef.Importance.HIGH)
                .width(ConfigDef.Width.LONG).group("Sink")
                .documentation("The insert mode, use insert or upsert expression, default is upsert.")
                .displayName("Insert mode")
                .validator(new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(String name, Object value) {
                        List<String> supports = Arrays.stream(InsertMode.values()).map(InsertMode::name).collect(Collectors.toList());
                        if (!supports.contains(value.toString().toUpperCase())) {
                            throw new ConfigException(name, value, "Only can be : " + Arrays.toString(InsertMode.values()));
                        }
                    }
                });

        keys.define(KEY_QUERY_TIMEOUT).type(ConfigDef.Type.LONG)
                .defaultValue(AsyncKuduClient.DEFAULT_SOCKET_READ_TIMEOUT_MS).importance(ConfigDef.Importance.HIGH)
                .width(ConfigDef.Width.LONG).group("Connection")
                .documentation(
                        "The timeout to use when waiting on data from a socket, @see:org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder.defaultSocketReadTimeoutMs")
                .displayName("The query timeout millis");

        keys.define(KEY_OPERATION_TIMEOUT).type(ConfigDef.Type.LONG)
                .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS).importance(ConfigDef.Importance.HIGH)
                .width(ConfigDef.Width.LONG).group("Connection")
                .documentation(
                        "The timeout used for user operations (using sessions and scanners), @see:org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder.defaultOperationTimeoutMs")
                .displayName("The operation timeout millis");

        keys.define(KEY_ADMIN_OPERATION_TIMEOUT).type(ConfigDef.Type.LONG)
                .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS).importance(ConfigDef.Importance.HIGH)
                .width(ConfigDef.Width.LONG).group("Connection")
                .documentation(
                        "The timeout used for administrative operations (e.g. createTable, deleteTable, etc), @See:org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder.defaultAdminOperationTimeoutMs")
                .displayName("The admin operation timeout millis");

        return keys;
    }

    public String getMappedTable(String tableName) {
        if (StringUtils.isEmpty(getString(KEY_TABLE_WHITELIST))) {
            return tableMap.getOrDefault(tableName, tableName.replace(".", "_"));
        }

        return tableMap.get(tableName);
    }

    private void analysisTableMap() {
        String[] configTables = getString(KEY_TABLE_WHITELIST).split(",");
        for (String configTable : configTables) {
            if (!configTable.contains(":")) {
                tableMap.put(configTable, configTable.replace(".", "_"));
                continue;
            }

            String[] map = configTable.split(":");
            if (map.length != 2) {
                throw new RuntimeException("Table pattern error, table item contains one ':' only.");
            }

            tableMap.put(map[0], map[1].replace(".", "_"));
        }

        log.info("Table sink map : {}", tableMap);
    }

    public enum InsertMode implements EnumeratedValue {

        /**
         * Use insert expression
         */
        INSERT("insert"),

        /**
         * Use upsert expression
         */
        UPSERT("upsert");

        private final String value;

        private InsertMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static InsertMode parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();
            for (InsertMode option : InsertMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static InsertMode parse(String value, String defaultValue) {
            InsertMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }
}
