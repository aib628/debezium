/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.writer;

import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.sink.KuduSinkConfig;
import io.debezium.connector.kudu.sink.operation.GenericOperation;
import io.debezium.connector.kudu.utils.KuduUtils;
import io.debezium.connector.kudu.utils.StringUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KuduWriter {

    @Getter
    private final KuduClient client;
    private final KuduSinkConfig config;

    private KuduWriter(KuduSinkConfig config) {
        this.config = config;
        this.client = KuduUtils.getSynchronousKuduClient(config);
    }

    public static KuduWriter builder(KuduSinkConfig config) {
        return new KuduWriter(config);
    }

    public KuduWriter build() {
        return this;
    }

    public void write(List<CDCEventLog> eventLogs) throws KuduException {
        if (log.isDebugEnabled()) {
            log.debug("CDC event logs:{}", eventLogs);
        }

        List<Operation> operations = new ArrayList<>(eventLogs.size());
        for (CDCEventLog eventLog : eventLogs) {
            String mappedTable = config.getMappedTable(eventLog.getCanonicalTableName());
            if (StringUtils.isEmpty(mappedTable)) {
                log.warn("Table sink not config : {}", mappedTable);
                continue; // Not mapped but table whitelist config.
            }

            log.debug("Begin to generate sink kudu operation.");
            GenericOperation genericOperation = GenericOperation.operate(eventLog, client, config);
            operations.add(genericOperation.get());
            if (genericOperation.needFlush) {
                flush(operations);
            }
        }

        eventLogs.clear();
        flush(operations);
    }

    private void flush(List<Operation> operations) throws KuduException {
        KuduSession session = KuduUtils.getSession(client, config);
        try {
            for (Operation operation : operations) {
                session.apply(operation);
                if (session.countPendingErrors() != 0) {
                    log.error("inserting to kudu error, there were the first few errors follow:");
                    RowErrorsAndOverflowStatus errorsAndStatus = session.getPendingErrors();
                    RowError[] errors = errorsAndStatus.getRowErrors();
                    for (int i = 0; i < Math.min(errors.length, 5); i++) {
                        log.error(errors[i].toString());
                    }

                    if (errorsAndStatus.isOverflowed()) {
                        log.error("error buffer overflowed: some errors were discarded");
                    }

                    throw new RuntimeException(String.format("inserting to kudu error, rows : %s", session.countPendingErrors()));
                }
            }
        }
        finally {
            session.flush();
            session.close();
            operations.clear();
        }
    }
}
