/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converter;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import lombok.extern.slf4j.Slf4j;

/**
 * create table type_test(
 * type_date date,
 * type_datetime datetime,
 * type_timestamp timestamp,
 * type_time time,
 * type_year year
 * );
 * <p>
 * insert into type_test values('2020-07-26','2020-07-26 10:00:00',now(),'10:00:00','2020');
 */
@Slf4j
public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    public static final Map<String, Function<Object, String>> SUPPORTED_DATA_TYPES = new HashMap<>();
    public static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    public static final String DEFAULT_YEAR_FORMAT = "yyyy";

    private String timestampFormat = DEFAULT_TIMESTAMP_FORMAT;
    private String dateTimeFormat = DEFAULT_DATETIME_FORMAT;
    private String dateFormat = DEFAULT_DATE_FORMAT;
    private String timeFormat = DEFAULT_TIME_FORMAT;
    private String yearFormat = DEFAULT_YEAR_FORMAT;

    private Map<String, Function<Object, String>> registerSupportedDateTypes() {
        Map<String, Function<Object, String>> supportedDateTypes = new HashMap<>();
        supportedDateTypes.put("YEAR", (value) -> {
            if (value instanceof Year) {
                return ((Year) value).format(DateTimeFormatter.ofPattern(yearFormat));
            }

            // year is convert to java.sql.Date when snapshot mode
            if (value instanceof java.sql.Date) {
                return new Date(((java.sql.Date) value).getTime()).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(yearFormat));
            }
            else if (value instanceof Date) {
                return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(yearFormat));
            }

            if (!(value instanceof String)) {
                log.warn("begin try intelligent conversion, but we don't want that to happen. please enhance it. year@{}", value.getClass().getCanonicalName());
            }

            return value.toString();
        });

        supportedDateTypes.put("DATE", (value) -> {
            if (value instanceof LocalDate) {
                return ((LocalDate) value).format(DateTimeFormatter.ofPattern(dateFormat));
            }

            if (value instanceof java.sql.Date) {
                return new Date(((java.sql.Date) value).getTime()).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(dateFormat));
            }
            else if (value instanceof Date) {
                return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(dateFormat));
            }

            if (!(value instanceof String)) {
                log.warn("begin try intelligent conversion, but we don't want that to happen. please enhance it. date@{}", value.getClass().getCanonicalName());
            }

            try {
                return intelligentTransformation(value.toString(), dateFormat);
            }
            catch (Exception e) {
                intelligentTransformationLogger(value);
            }

            return value.toString();
        });

        supportedDateTypes.put("TIME", (value) -> {
            if (value instanceof LocalTime) {
                return ((LocalTime) value).format(DateTimeFormatter.ofPattern(timeFormat));
            }

            if (value instanceof Duration) {
                Instant instant = new Date(((Duration) value).toMillis()).toInstant();
                LocalDateTime localDateTime = instant.atZone(ZoneId.of("UTC")).toLocalDateTime();
                return localDateTime.format(DateTimeFormatter.ofPattern(timeFormat));
            }

            if (value instanceof java.sql.Date) {
                return new Date(((java.sql.Date) value).getTime()).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(timeFormat));
            }
            else if (value instanceof Date) {
                return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(timeFormat));
            }

            if (!(value instanceof String)) {
                log.warn("begin try intelligent conversion, but we don't want that to happen. please enhance it. time@{}", value.getClass().getCanonicalName());
            }

            try {
                return intelligentTransformation(value.toString(), timeFormat);
            }
            catch (Exception e) {
                intelligentTransformationLogger(value);
            }

            return value.toString();
        });

        supportedDateTypes.put("DATETIME", (value) -> {
            if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).format(DateTimeFormatter.ofPattern(dateTimeFormat));
            }

            // datetime is convert to java.sql.Timestamp when snapshot mode
            if (value instanceof Timestamp) {
                return ((Timestamp) value).toLocalDateTime().format(DateTimeFormatter.ofPattern(dateTimeFormat));
            }

            if (value instanceof java.sql.Date) {
                return new Date(((java.sql.Date) value).getTime()).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(dateTimeFormat));
            }
            else if (value instanceof Date) {
                return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(dateTimeFormat));
            }

            if (!(value instanceof String)) {
                log.warn("begin try intelligent conversion, but we don't want that to happen. please enhance it. datetime@{}", value.getClass().getCanonicalName());
            }

            try {
                return intelligentTransformation(value.toString(), dateTimeFormat);
            }
            catch (Exception e) {
                intelligentTransformationLogger(value);
            }

            return value.toString();
        });

        supportedDateTypes.put("TIMESTAMP", (value) -> {
            if (value instanceof ZonedDateTime) {
                return ((ZonedDateTime) value).withZoneSameInstant(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(timestampFormat));
            }

            // timestamp is convert to java.sql.Timestamp when snapshot mode
            if (value instanceof Timestamp) {
                return ((Timestamp) value).toLocalDateTime().format(DateTimeFormatter.ofPattern(timestampFormat));
            }

            if (value instanceof java.sql.Date) {
                return new Date(((java.sql.Date) value).getTime()).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(timestampFormat));
            }
            else if (value instanceof Date) {
                return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(timestampFormat));
            }

            if (!(value instanceof String)) {
                log.warn("begin try intelligent conversion, but we don't want that to happen. please enhance it. timestamp@{}", value.getClass().getCanonicalName());
            }

            try {
                return intelligentTransformation(value.toString(), timestampFormat);
            }
            catch (Exception e) {
                intelligentTransformationLogger(value);
            }

            return value.toString();
        });

        return supportedDateTypes;
    }

    private void intelligentTransformationLogger(Object value) {
        String typeName = value.getClass().getCanonicalName();
        String pattern = DateTimeParser.parser(value.toString()).dateTimePattern;
        log.warn("Intelligent convert failed when using pattern {}, value : {}@{}", pattern, value, typeName);
    }

    @Override
    public void configure(Properties props) {
        this.timestampFormat = props.getProperty("format.timestamp", DEFAULT_TIMESTAMP_FORMAT);
        this.dateTimeFormat = props.getProperty("format.datetime", DEFAULT_DATETIME_FORMAT);
        this.dateFormat = props.getProperty("format.date", DEFAULT_DATE_FORMAT);
        this.timeFormat = props.getProperty("format.time", DEFAULT_TIME_FORMAT);
        this.yearFormat = props.getProperty("format.year", DEFAULT_YEAR_FORMAT);

        Map<String, Function<Object, String>> supportedDateTypes = registerSupportedDateTypes();
        String namelist = props.getProperty("type.namelist");
        if (namelist == null || namelist.trim().length() == 0) {
            return;
        }

        String[] types = namelist.toUpperCase().split(",");
        for (String type : types) {
            if (!supportedDateTypes.containsKey(type)) {
                log.warn("Not supported date type : {}, {} are supported only, ignore default.", type, supportedDateTypes.keySet());
                continue;
            }

            log.info("registered {} converter: x -> string", type);
            SUPPORTED_DATA_TYPES.put(type, supportedDateTypes.get(type));
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (SUPPORTED_DATA_TYPES.keySet().stream().noneMatch(typeName -> typeName.equalsIgnoreCase(column.typeName()))) {
            return;
        }

        registration.register(SchemaBuilder.string(), rawValue -> {
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

    public String intelligentTransformation(String dateTime, String targetPattern) throws ParseException {
        DateTimeParser parser = DateTimeParser.parser(dateTime);
        if (!dateTime.matches(parser.getDateTimeRegex())) {
            log.info("warning: the value is {}, try use pattern : {} to transform.", dateTime, parser.dateTimePattern);
        }

        Matcher matcher = parser.getDateTimeRegexPattern().matcher(dateTime);
        if (matcher.find()) {
            String dateTimePart = matcher.group(1);
            String zonePart = dateTime.replace(dateTimePart, "");
            if (!zonePart.isEmpty() && (zonePart.startsWith("+") || zonePart.startsWith("-"))) {
                Date date = new SimpleDateFormat(parser.dateTimePattern).parse(dateTimePart);
                return date.toInstant().atZone(ZoneOffset.of(zonePart)).withZoneSameInstant(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(targetPattern));
            }
        }

        // try transform.
        Date parsedDate = new SimpleDateFormat(parser.dateTimePattern).parse(dateTime);
        if (dateTime.contains("Z")) {
            return parsedDate.toInstant().atZone(ZoneId.of("UTC")).withZoneSameInstant(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(targetPattern));
        }

        return new SimpleDateFormat(targetPattern).format(parsedDate);
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

    public static class DateTimeParser {

        private final String dateValue;
        private String dateTimePattern;
        private String dateTimeRegex;

        DateTimeParser(String dateValue) {
            this.dateValue = dateValue;
            generateDateTimePattern(dateValue);
        }

        public static DateTimeParser parser(String dateValue) {
            return new DateTimeParser(dateValue);
        }

        public String getDateValue() {
            return this.dateValue;
        }

        public String getDateTimeRegex() {
            return dateTimeRegex;
        }

        public String getDateTimePattern() {
            return dateTimePattern;
        }

        public Pattern getDateTimeRegexPattern() {
            return Pattern.compile(dateTimeRegex);
        }

        private void generateDateTimePattern(String dateValue) {
            StringBuilder regex = new StringBuilder();
            StringBuilder pattern = new StringBuilder();
            if (dateValue.contains("-")) {
                pattern.append(DEFAULT_DATE_FORMAT);
                regex.append("[0-9]{4}-[01][0-9]-[0-3][0-9]");
            }

            if (dateValue.contains("T")) {
                pattern.append("'T'");
                regex.append("T");
            }
            else if (dateValue.contains(":") || dateValue.contains(".")) {
                pattern.append(" ");
                regex.append(" ");
            }

            if (dateValue.contains(":")) {
                pattern.append(DEFAULT_TIME_FORMAT);
                regex.append("[0-2][0-9](:[0-5][0-9]){2}");
            }

            if (dateValue.contains(".")) {
                pattern.append(".");
                regex.append("\\.");

                String millisTime = dateValue.substring(dateValue.lastIndexOf("."));
                for (int i = 0; i < millisTime.length() - 1; i++) {
                    if (dateValue.endsWith("Z") && i == millisTime.length() - 2) {
                        pattern.append("X");
                        regex.append("Z");
                        continue;
                    }

                    pattern.append("S");
                    regex.append("[0-9]");
                }
            }
            else if (dateValue.endsWith("Z")) {
                pattern.append("X");
                regex.append("Z");
            }

            this.dateTimePattern = pattern.toString().trim();
            this.dateTimeRegex = "(" + regex.toString().trim() + ").*";
        }
    }
}
