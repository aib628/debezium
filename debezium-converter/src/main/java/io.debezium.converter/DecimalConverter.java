/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converter;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import lombok.extern.slf4j.Slf4j;

/**
 * KUDU：BOOL, INT8, INT16, INT32, BIGINT, INT64, FLOAT, DOUBLE, STRING, BINARY, TIMESTAMP.
 * DEBEZIUM：INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT
 */
@Slf4j
public class DecimalConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    public static final Map<String, Function<Object, Object>> SUPPORTED_DATA_TYPES = new HashMap<>();

    private Map<String, Function<Object, Object>> registerSupportedDateTypes() {
        Map<String, Function<Object, Object>> supportedDateTypes = new HashMap<>();
        supportedDateTypes.put("FLOAT", (value) -> {
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).floatValue();
            }

            if (value instanceof Float) {
                return value;
            }

            return Float.parseFloat(value.toString());
        });

        supportedDateTypes.put("DOUBLE", (value) -> {
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).doubleValue();
            }

            if (value instanceof Double) {
                return value;
            }

            return Double.parseDouble(value.toString());
        });

        return supportedDateTypes;
    }

    @Override
    public void configure(Properties props) {
        Map<String, Function<Object, Object>> supportedDataTypes = registerSupportedDateTypes();

        String namelist = props.getProperty("type.namelist");
        if (namelist == null || namelist.trim().length() == 0) {
            return;
        }

        String[] types = namelist.toUpperCase().split(",");
        for (String type : types) {
            if (!supportedDataTypes.containsKey(type)) {
                log.warn("Not supported data type : {}, {} are supported only, ignore default.", type, supportedDataTypes.keySet());
                continue;
            }

            log.info("registered decimal converter {}", type);
            SUPPORTED_DATA_TYPES.put(type, supportedDataTypes.get(type));
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (SUPPORTED_DATA_TYPES.keySet().stream().noneMatch(typeName -> typeName.equalsIgnoreCase(column.typeName()))) {
            return;
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.float64();
        if ("Float".equalsIgnoreCase(column.typeName())) {
            schemaBuilder = SchemaBuilder.float32();
        }

        registration.register(schemaBuilder, rawValue -> {
            if (rawValue == null && column.hasDefaultValue()) {
                if (column.defaultValue() == null) {
                    return null;
                }

                return SUPPORTED_DATA_TYPES.get(column.typeName()).apply(column.defaultValue());
            }
            else if (rawValue == null) {
                return null; // null even if not optional
            }

            if (log.isDebugEnabled()) {
                printRelationalColumnDebugInfo(column, rawValue);
            }

            return SUPPORTED_DATA_TYPES.get(column.typeName()).apply(rawValue);
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
