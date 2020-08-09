/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.sink.parser;

import java.util.function.Supplier;

public class AbsentNodeException extends RuntimeException {

    public AbsentNodeException(String nodeName) {
        super(nodeName + " unexpected be null.");
    }

    public static Supplier<AbsentNodeException> supplier(String nodeName) {
        return () -> new AbsentNodeException(nodeName);
    }

}
