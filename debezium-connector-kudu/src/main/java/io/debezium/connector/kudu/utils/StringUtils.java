/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.utils;

public class StringUtils {

    public static boolean isEmpty(String str) {
        if (str == null) {
            return true;
        }

        if (str.trim().length() == 0) {
            return true;
        }

        return false;
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }
}
