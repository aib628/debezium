/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.parser;

import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.connector.kudu.CDCEventLog;

public class AvroSinkRecordParser implements SinkRecordParser {

    @Override
    public CDCEventLog parse(SinkRecord sinkRecord) throws JsonProcessingException {
        return null;
    }

}
