/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kudu.client.KuduException;
import org.slf4j.MDC;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.Version;
import io.debezium.connector.kudu.sink.parser.JsonSinkRecordParser;
import io.debezium.connector.kudu.sink.writer.KuduWriter;
import io.debezium.connector.kudu.utils.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KuduSinkTask extends SinkTask {

    private final String TRACE_ID = "dbz.traceId";
    private KuduWriter writer;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        log.info("Starting kudu sink task...");
        KuduSinkConfig config = new KuduSinkConfig(settings);
        this.writer = KuduWriter.builder(config);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        MDC.put(TRACE_ID, UUID.randomUUID().toString().replace("-", "")); // generate global trace id

        // when 'tombstones.on.delete' is true, will generate null event
        List<SinkRecord> records = sinkRecords.stream().filter((it) -> {
            if (it.key() == null || it.value() == null) {
                return false;
            }

            return StringUtils.isNotEmpty(it.value().toString()) && StringUtils.isNotEmpty(it.key().toString());
        }).collect(Collectors.toList());

        if (sinkRecords.size() > records.size()) {
            log.info("Filtered {} invalid records, key or value is null, discard default.", sinkRecords.size() - records.size());
        }

        if (records.isEmpty()) {
            return;
        }

        try {
            doPut(records);
        }
        catch (KuduException e) {
            log.error("Kudu exception occur:" + e.getMessage(), e);
            throw new RuntimeException("Kudu exception occur:" + e.getMessage(), e);
        }
        catch (Exception e) {
            log.error("Unexpected exception occur:" + e.getMessage(), e);
            throw new RuntimeException("Unexpected exception occur:" + e.getMessage(), e);
        }
        finally {
            MDC.remove(TRACE_ID);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping kudu sink task.");
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.info("Flushing data to kudu with the following offsets: {}", currentOffsets);
    }

    private void doPut(List<SinkRecord> sinkRecords) throws KuduException {
        long begin = System.currentTimeMillis();
        log.info("Putting {} records to Kudu", sinkRecords.size());
        writer.write(sinkRecords.stream().map(this::transform).collect(Collectors.toList()));
        log.info("Putting done. {} committed, cost {} ms", sinkRecords.size(), System.currentTimeMillis() - begin);
    }

    private CDCEventLog transform(SinkRecord record) {
        if (log.isDebugEnabled()) {
            printRecordInfo(record);
        }

        try {
            return new JsonSinkRecordParser().parse(record);
        }
        catch (JsonProcessingException e) {
            log.error("record parse failed, " + e.getMessage(), e);
            throw new RuntimeException("Transform failed, record parse failed:" + e.getMessage(), e);
        }
        catch (Exception e) {
            log.error("record parse unexpected error, " + e.getMessage(), e);
            throw new RuntimeException("Transform failed, record parse unexpected error:" + e.getMessage(), e);
        }
    }

    private void printRecordInfo(SinkRecord record) {
        JsonConverter keyJsonConverter = new JsonConverter();
        keyJsonConverter.configure(new HashMap<>(), true);
        log.debug("key:{}", record.key()); // java.lang.String:{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"}],"optional":false,"name":"dbserver1.inventory.customers.Key"},"payload":{"id":1004}}
        log.debug("keySchema:{}", keyJsonConverter.asJsonSchema(record.keySchema())); // org.apache.kafka.connect.data.ConnectSchema:{"type":"string","optional":true}

        JsonConverter valueJsonConverter = new JsonConverter();
        valueJsonConverter.configure(new HashMap<>(), false);
        log.debug("value:{}", record.value()); // java.lang.String:{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"first_name"},{"type":"string","optional":false,"field":"last_name"},{"type":"string","optional":false,"field":"email"}],"optional":true,"name":"dbserver1.inventory.customers.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"first_name"},{"type":"string","optional":false,"field":"last_name"},{"type":"string","optional":false,"field":"email"}],"optional":true,"name":"dbserver1.inventory.customers.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.customers.Envelope"},"payload":{"before":{"id":1004,"first_name":"555","last_name":"Kretchmar","email":"annek@noanswer.org"},"after":{"id":1004,"first_name":"666","last_name":"Kretchmar","email":"annek@noanswer.org"},"source":{"version":"1.2.0.Final","connector":"mysql","name":"dbserver1","ts_ms":1595131183000,"snapshot":"false","db":"inventory","table":"customers","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":2130,"row":0,"thread":8,"query":null},"op":"u","ts_ms":1595131183469,"transaction":null}}
        log.debug("valueSchema:{}", valueJsonConverter.asJsonSchema(record.valueSchema())); // org.apache.kafka.connect.data.ConnectSchema:{"type":"string","optional":true}
    }
}
