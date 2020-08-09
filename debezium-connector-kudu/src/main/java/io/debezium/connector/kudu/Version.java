/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu;

import java.io.InputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Version {
    private static String version = "unknown";
    private static final String VERSION_PATH = "/debezium-connect-kudu-version.properties";

    static {
        readVersion();
    }

    public static String getVersion() {
        return version;
    }

    private static void readVersion() {
        try (InputStream stream = Version.class.getResourceAsStream(VERSION_PATH)) {
            if (stream == null) {
                log.warn("Failed to find version and using '{}' for version; expecting to find file in '{}'", "unknown", VERSION_PATH);
            }

            Properties props = new Properties();
            props.load(stream);
            version = props.getProperty("version", "unknown").trim();
            log.trace("Found version '{}' in '{}' using classloader for {}", version, VERSION_PATH, Version.class.getName());
        }
        catch (Exception e) {
            log.warn("Failed to find version and using '{}' for version; error reading file in '{}'", "unknown", VERSION_PATH, e);
        }
    }

    private static String modifiedPackageName(Class<?> clazz) {
        return clazz.getPackage().getName().replaceAll("[.]", "/");
    }

    public static void main(String[] args) {
        log.info(getVersion());
    }
}
