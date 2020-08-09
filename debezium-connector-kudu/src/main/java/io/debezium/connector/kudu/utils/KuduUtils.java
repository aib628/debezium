/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu.utils;

import java.util.Arrays;
import java.util.List;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration;

import io.debezium.connector.kudu.KuduConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KuduUtils {

    public static KuduClient getSynchronousKuduClient(KuduConfig config) {
        List<String> kuduMasters = Arrays.asList(config.getString(KuduConfig.KEY_MASTER_ADDRESSES).split(","));
        return new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasters)
                .defaultSocketReadTimeoutMs(config.getLong(KuduConfig.KEY_QUERY_TIMEOUT))
                .defaultOperationTimeoutMs(config.getLong(KuduConfig.KEY_OPERATION_TIMEOUT))
                .defaultAdminOperationTimeoutMs(config.getLong(KuduConfig.KEY_ADMIN_OPERATION_TIMEOUT))
                .build()
                .syncClient();
    }

    public static KuduSession getSession(KuduClient client, KuduConfig config) {
        KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(config.getInt(KuduConfig.KEY_BATCH_SIZE));
        kuduSession.setTimeoutMillis(config.getLong(KuduConfig.KEY_OPERATION_TIMEOUT));
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.valueOf(config.getString(KuduConfig.KEY_FLUSH_MODE).toUpperCase()));
        return kuduSession;
    }

    public static AsyncKuduScanner.ReadMode getReadMode(String readMode) {
        if (AsyncKuduScanner.ReadMode.READ_LATEST.name().equalsIgnoreCase(readMode)) {
            return AsyncKuduScanner.ReadMode.READ_LATEST;
        }
        else {
            return AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT;
        }
    }

    public static void closeSession(KuduSession kuduSession) {
        if (kuduSession.isClosed()) {
            return;
        }

        try {
            kuduSession.close();
        }
        catch (KuduException e) {
            log.error("close session error, for reason:" + e.getMessage(), e);
        }
    }
}
