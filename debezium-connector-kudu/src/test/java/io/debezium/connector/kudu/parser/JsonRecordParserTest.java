package io.debezium.connector.kudu.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.connector.kudu.CDCEventLog;
import io.debezium.connector.kudu.sink.parser.JsonSinkRecordParser;

public class JsonRecordParserTest {

    public static void main(String[] args) throws JsonProcessingException, FileNotFoundException {
        File source = new File(JsonRecordParserTest.class.getResource("/").getPath(), "Debezium.source.sample");
        String text = new BufferedReader(new FileReader(source)).lines().collect(Collectors.joining("\n"));
        String key = text.substring(text.indexOf("input key:") + 10, text.indexOf("input value:")).trim();
        String value = text.substring(text.indexOf("input value:") + 12).trim();

        SinkRecord sinkRecord = new SinkRecord("null", 0, null, key, null, value, 0);
        CDCEventLog eventLog = new JsonSinkRecordParser().parse(sinkRecord);
        System.out.println(eventLog.toString());
    }

}
