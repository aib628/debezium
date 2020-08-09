/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kudu.client.SessionConfiguration;

import io.debezium.connector.kudu.ConfigKeys;
import io.debezium.connector.kudu.KuduConfig;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KuduSinkConfig extends KuduConfig {

    public KuduSinkConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return sinkConfigDefinition().toConfigDef();
    }

    private static ConfigKeys sinkConfigDefinition() {
        ConfigKeys keys = commonConfigDefinition();
        keys.define(KEY_FLUSH_MODE, ConfigDef.Type.STRING)
                .defaultValue(SessionConfiguration.FlushMode.MANUAL_FLUSH.name()).importance(ConfigDef.Importance.HIGH)
                .width(ConfigDef.Width.LONG).group("Sink")
                .documentation("The session flush mode.")
                .displayName("Flush mode")
                .validator(new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(String name, Object value) {
                        List<String> supports = Arrays.stream(SessionConfiguration.FlushMode.values()).map(SessionConfiguration.FlushMode::name)
                                .collect(Collectors.toList());
                        if (!supports.contains(value.toString().toUpperCase())) {
                            throw new ConfigException(name, value, "Only can be : " + Arrays.toString(SessionConfiguration.FlushMode.values()));
                        }
                    }
                });

        keys.define(KEY_BATCH_SIZE, ConfigDef.Type.INT)
                .defaultValue(1_000).importance(ConfigDef.Importance.LOW)
                .width(ConfigDef.Width.MEDIUM).group("Sink")
                .documentation("The number of operations that can be buffered.")
                .displayName("Batch operation size");

        return keys;
    }
}
