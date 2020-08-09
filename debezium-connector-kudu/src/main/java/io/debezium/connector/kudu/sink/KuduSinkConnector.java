/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import io.debezium.connector.kudu.KuduConfig;
import io.debezium.connector.kudu.Version;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KuduSinkConnector extends SinkConnector {
    private KuduConfig config;

    @Override
    public void start(Map<String, String> props) {
        this.config = new KuduSinkConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KuduSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);

        for (int i = 0; i < maxTasks; ++i) {
            configs.add(config.originalsStrings());
        }

        return configs;
    }

    @Override
    public void stop() {
        log.info("Kudu sink connector stop method invoked.");
    }

    @Override
    public ConfigDef config() {
        return KuduSinkConfig.config();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
