/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converter;

import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import lombok.extern.slf4j.Slf4j;

/**
 * Impala不可以建立包含binary类型KUDU外连表，将Binary转为String存储
 */
@Slf4j
public class BinaryConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    @Override
    public void configure(Properties props) {

    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (!"BINARY".equalsIgnoreCase(column.typeName()) && !"VARBINARY".equalsIgnoreCase(column.typeName())) {
            return;
        }

        registration.register(SchemaBuilder.string(), rawValue -> {
            if (rawValue == null && column.hasDefaultValue()) {
                if (column.defaultValue() == null) {
                    return null;
                }

                Object defaultValue = column.defaultValue();
                if (defaultValue instanceof byte[]) {
                    return new String((byte[]) defaultValue);
                }

                return defaultValue.toString();
            }
            else if (rawValue == null) {
                return null; // null even if not optional
            }

            if (log.isDebugEnabled()) {
                printRelationalColumnDebugInfo(column, rawValue);
            }

            if (rawValue instanceof byte[]) {
                return new String((byte[]) rawValue);
            }

            return rawValue.toString();
        });
    }

    private void printRelationalColumnDebugInfo(RelationalColumn column, Object rawValue) {
        log.info("dataCollection:{}", column.dataCollection()); // dataCollection:inventory.type_test
        log.info("name:{}", column.name()); // type_date type_datetime type_timestamp type_time type_year
        log.info("typeName:{}", column.typeName()); // DATE DATETIME TIMESTAMP TIME YEAR
        log.info("jdbcType:{}", column.jdbcType()); // 91 93 2014 92 4
        log.info("nativeType:{}", column.nativeType()); // -1 -1 -1 -1 -1
        log.info("typeExpression:{}", column.typeExpression()); // DATE DATETIME TIMESTAMP TIME YEAR
        log.info("rawValue:{}@{}", rawValue, rawValue.getClass().getCanonicalName()); // 2020-07-26@java.time.LocalDate 2020-07-26T10:00@java.time.LocalDateTime 2020-07-26T10:33:50Z@java.time.ZonedDateTime PT10H@java.time.Duration 2020@java.time.Year
    }
}
