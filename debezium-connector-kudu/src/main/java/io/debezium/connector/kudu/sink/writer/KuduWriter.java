/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.writer;

import java.util.LinkedList;
import java.util.List;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.KuduConfig;
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

    @Getter
    private final KuduSinkConfig config;

    private List<Operation> operationBuffer;

    private KuduWriter(KuduSinkConfig config) {
        this.config = config;
        this.operationBuffer = new LinkedList<>();
        this.client = KuduUtils.getSynchronousKuduClient(config);
    }

    public static KuduWriter builder(KuduSinkConfig config) {
        return new KuduWriter(config);
    }

    public void write(CDCEventLog eventLog) throws KuduException {
        log.debug("CDC event logs:{}", eventLog);
        String mappedTable = config.getMappedTable(eventLog.getCanonicalTableName());
        if (StringUtils.isEmpty(mappedTable)) {
            log.warn("Table sink not config : {}", mappedTable);
            return; // Not mapped but table whitelist config.
        }

        GenericOperation genericOperation = GenericOperation.operate(eventLog, client, config);
        if (genericOperation.needFlush) {
            flush(); // flush operation 单独提交，防止比如目标数据库表DDL提交修改成功，而Offset未提交成功导致的重试操作无法通过
            commitOperation(genericOperation.get());
            flush();
        }

        commitOperation(genericOperation.get());
        if (shouldFlush()) {
            flush();
        }
    }

    private void commitOperation(Operation operation) {
        operationBuffer.add(operation);
    }

    private boolean shouldFlush() {
        if (operationBuffer.size() >= config.getInt(KuduConfig.KEY_BATCH_SIZE)) {
            return true;
        }

        return false;
    }

    private List<Operation> reassignBuffer() {
        List<Operation> operations = operationBuffer;
        operationBuffer = new LinkedList<>();
        return operations;
    }

    public synchronized void flush() throws KuduException {
        List<Operation> operations = reassignBuffer();
        if (operations.isEmpty()) {
            return;
        }

        long beginTime = System.currentTimeMillis();
        KuduSession session = KuduUtils.getSession(client, config);
        if (KuduUtils.debugLogOpened(config)) {
            log.info("obtain session costs : {} ms", System.currentTimeMillis() - beginTime);
        }

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

            if (KuduUtils.manualFlushMode(config)) { // manual_flush need to flush, other not.
                session.flush(); // flush to disk. when manual_flush must call it
                log.info("flush {} operations costs : {} ms", operations.size(), System.currentTimeMillis() - beginTime);
            }
        }
        finally {
            session.close();
            operations.clear();
        }
    }
}
