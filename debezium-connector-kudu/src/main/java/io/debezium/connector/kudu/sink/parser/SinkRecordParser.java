/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.parser;

import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.connector.kudu.CDCEventLog;

public interface SinkRecordParser {

    String KEY_NAME = "name";
    String KEY_FIELD = "field";
    String KEY_FIELDS = "fields";
    String KEY_OPTIONAL = "optional";
    String KEY_VERSION = "version";

    /**
     * Parses the key and values based on the key and values schema.
     */
    CDCEventLog parse(SinkRecord sinkRecord) throws JsonProcessingException;

}
