/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converter;

import static io.debezium.converter.TimestampConverter.DEFAULT_DATETIME_FORMAT;
import static io.debezium.converter.TimestampConverter.DEFAULT_DATE_FORMAT;
import static io.debezium.converter.TimestampConverter.DEFAULT_TIMESTAMP_FORMAT;
import static io.debezium.converter.TimestampConverter.DEFAULT_TIME_FORMAT;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimestampConverterTest {

    @Test
    public void testGenerateDataTimePattern() {
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("2020-08-06"));
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("2020-08-06 14:10:10.222"));
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("2020-08-06T14:10:10.222"));
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("2020-08-06Z"));
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("2020-08-06T14:10:10Z"));
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("2020-08-06T14:10:10.222Z"));
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("2020-08-06T14:10:10+08:00"));
        printDateTimeParserResult(TimestampConverter.DateTimeParser.parser("0000-00-00 00:00:00"));
    }

    @Test
    public void testIntelligentTransformation() throws ParseException {
        TimestampConverter timestampConverter = new TimestampConverter();
        log.info(timestampConverter.intelligentTransformation("2020-08-06T14:10:10.222Z", DEFAULT_TIMESTAMP_FORMAT));
        log.info(timestampConverter.intelligentTransformation("2020-08-06T14:10:10.222", DEFAULT_DATETIME_FORMAT));
        log.info(timestampConverter.intelligentTransformation("2020-08-06 14:10:10.222", DEFAULT_DATETIME_FORMAT));
        log.info(timestampConverter.intelligentTransformation("2020-08-06Z", DEFAULT_DATE_FORMAT));
        log.info(timestampConverter.intelligentTransformation("2020-08-06", DEFAULT_DATE_FORMAT));
        log.info(timestampConverter.intelligentTransformation("14:10:10Z", DEFAULT_TIME_FORMAT));
        log.info(timestampConverter.intelligentTransformation("14:10:10", DEFAULT_TIME_FORMAT));
        log.info(timestampConverter.intelligentTransformation("14:10:10.222Z", DEFAULT_TIME_FORMAT));
        log.info(timestampConverter.intelligentTransformation("14:10:10.222", DEFAULT_TIME_FORMAT));

        // 对于类似1970-01-01T08:00:01+08:00这样的时间，需要解析出时间部分，然后根据时间偏移得到0时区时间
        log.info(timestampConverter.intelligentTransformation("1970-01-01T08:00:01+07:00", DEFAULT_TIMESTAMP_FORMAT));
        log.info(timestampConverter.intelligentTransformation("1970-01-01T08:00:01-08:00", DEFAULT_TIMESTAMP_FORMAT));
        log.info(timestampConverter.intelligentTransformation("0000-00-00 00:00:00", DEFAULT_TIMESTAMP_FORMAT));
    }

    @Test
    public void testLocalDateTime() {
        infoLogger("===============2020-08-06 14:10:10.222=============="); // LocalDate LocalTime LocalDateTime Instance等皆为不可变对象，对同一对象重复加时区计算不影响结果准确性
        LocalDateTime dateTime = LocalDateTime.of(2020, 8, 6, 14, 10, 10, 222000000); // 2020-08-06 14:10:10.222

        infoLogger("1============== LocalDateTime DateTimeFormatter.ofPattern Locale -> Locale不会启作用", true);
        infoLogger(dateTime.format(DateTimeFormatter.ofPattern(DEFAULT_TIMESTAMP_FORMAT, Locale.ENGLISH)));
        infoLogger(dateTime.format(DateTimeFormatter.ofPattern(DEFAULT_TIMESTAMP_FORMAT, Locale.getDefault())));

        infoLogger("2============== LocalDateTime.atZone -> 只新增时区信息，不改变时间值。时间值相同，即使时区不同，format结果亦相同", true);
        infoLogger(dateTime.atZone(ZoneId.of("America/New_York")));
        infoLogger(dateTime.atZone(ZoneId.of("America/New_York")).toLocalDateTime()); // 相当于直接丢掉了时区
        infoLogger(dateTime.atZone(ZoneId.of("America/New_York")).toOffsetDateTime()); // 转换为Offset表示方式
        infoLogger(dateTime.atZone(ZoneId.of("America/New_York")).withZoneSameInstant(ZoneId.systemDefault()));

        infoLogger(dateTime.atZone(ZoneId.systemDefault()));
        infoLogger(dateTime.atZone(ZoneId.systemDefault()).toLocalDateTime());
        infoLogger(dateTime.atZone(ZoneId.systemDefault()).toOffsetDateTime());
        infoLogger(dateTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneId.systemDefault()));

        infoLogger("3============== ZonedDateTime DateTimeFormatter.ofPattern Locale -> Locale不会启作用", true);
        infoLogger(dateTime.atZone(ZoneId.of("America/New_York")).format(DateTimeFormatter.ofPattern(DEFAULT_TIMESTAMP_FORMAT, Locale.ENGLISH)));
        infoLogger(dateTime.atZone(ZoneId.of("America/New_York")).format(DateTimeFormatter.ofPattern(DEFAULT_TIMESTAMP_FORMAT, Locale.CHINA)));
        infoLogger(dateTime.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(DEFAULT_TIMESTAMP_FORMAT, Locale.ENGLISH)));
        infoLogger(dateTime.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(DEFAULT_TIMESTAMP_FORMAT, Locale.CHINA)));

        infoLogger("4==============　LocalDateTime.atZone(ZoneDateTime).toInstant -> Instance固定为0时区时间值，其中Z便代表0时区之意", true);
        infoLogger(dateTime.atZone(ZoneId.of("America/New_York")).toInstant());
        infoLogger(Date.from(dateTime.atZone(ZoneId.of("America/New_York")).toInstant()));

        infoLogger(dateTime.atZone(ZoneId.systemDefault()).toInstant());
        infoLogger(Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant()));

        infoLogger(new Timestamp(1596694210222L).toLocalDateTime()); // 2020-08-06 14:10:10.222
        infoLogger(new Timestamp(1596694210222L).toInstant().atZone(ZoneId.systemDefault())); // 2020-08-06 14:10:10.222
        infoLogger(new Date(1596694210222L).toInstant().atZone(ZoneId.systemDefault())); // 2020-08-06 14:10:10.222

        infoLogger("5============== LocalDateTime.toInstant LocalDateTime.toInstant.toEpochMilli LocalDateTime.toInstant.atZone", true);
        infoLogger("============== LocalDateTime.toInstant 将LocalDateTime转为指定时区的0时区瞬时时间", false);
        infoLogger("============== LocalDateTime.toInstant.toEpochMilli 将LocalDateTime转为指定时区的0时区瞬时时间　然后得出新纪元以来的时间毫秒数表示", false);
        infoLogger("============== LocalDateTime.toInstant.atZone 将LocalDateTime转为指定时区的0时区瞬时时间　然后再转换为指定时区的时间表示", false);

        infoLogger(dateTime.toInstant(ZoneOffset.UTC)); // 2020-08-06T14:10:10.222Z
        infoLogger(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli()); // 1596723010222 -> 2020-08-06T22:10:10.222
        infoLogger(dateTime.toInstant(ZoneOffset.UTC).atZone(ZoneId.systemDefault())); // 2020-08-06T22:10:10.222+08:00[Asia/Shanghai]
        infoLogger(dateTime.toInstant(ZoneOffset.of("+08"))); // 2020-08-06T06:10:10.222Z
        infoLogger(dateTime.toInstant(ZoneOffset.of("+08")).toEpochMilli()); // 1596694210222 -> 2020-08-06T14:10:10.222
        infoLogger(dateTime.toInstant(ZoneOffset.of("+08")).atZone(ZoneId.systemDefault())); // 2020-08-06T14:10:10.222+08:00[Asia/Shanghai]
    }

    private void infoLogger(Object obj) {
        infoLogger(obj, false);
    }

    private void infoLogger(Object obj, boolean nextLine) {
        if (nextLine) {
            log.info("");
        }

        if (obj == null) {
            log.info(null);
        }
        else {
            log.info(obj.toString());
        }
    }

    private void printDateTimeParserResult(TimestampConverter.DateTimeParser parser) {
        boolean validate = parser.getDateValue().matches(parser.getDateTimeRegex());
        log.info("dateTime:{}, pattern:{}, regex:{}, validate:{}", parser.getDateValue(), parser.getDateTimePattern(), parser.getDateTimeRegex(), validate);
    }
}
