/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu;

import java.util.HashMap;
import java.util.Map;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.kudu.sink.KuduSinkConfig;
import io.debezium.connector.kudu.utils.KuduUtils;

/**
 * <pre>
 * create table type_test(
 *     id int primary key auto_increment,
 *     type_tinyint tinyint,
 *     type_smallint smallint,
 *     type_mediumint mediumint,
 *     type_int int,
 *     type_bigint bigint,
 *     type_float float,
 *     type_double double,
 *     type_real real,
 *     type_bool bool,
 *     type_boolean boolean,
 *     type_numeric numeric,
 *     type_decimal decimal,
 *     type_date date,
 *     type_datetime datetime,
 *     type_timestamp timestamp,
 *     type_time time,
 *     type_year year,
 *     type_binary binary
 * );
 *
 * insert into type_test values(1,12,13,14,15,16,17.5,18.5,1,true,false,19.5,20.5,now(),now(),now(),'10:00:00','2020','');
 * </pre>
 */
public class KuduTools {

    private KuduClient client;
    public final String CREATE_IMPALATABLE_ON_KUDU = "CREATE EXTERNAL TABLE IF NOT EXISTS %s STORED AS KUDU TBLPROPERTIES ('kudu.table_name'='%s')";

    @Before
    public void initKuduClient() {
        this.client = KuduUtils.getSynchronousKuduClient(getKuduConfig());
    }

    @Test
    public void dropKuduTable() throws KuduException {
        client.deleteTable("inventory_type_test");
    }

    @Test
    public void dropAllTables() throws KuduException {
        for (String table : getKuduConfig().getTableMap().values()) {
            try {
                client.deleteTable(table);
            }
            catch (Exception e) {
                // ignore
            }
        }
    }

    private KuduConfig getKuduConfig() {
        Map<String, String> params = new HashMap<>();
        params.put(KuduConfig.KEY_MASTER_ADDRESSES, "psd-hadoop041,psd-hadoop042,psd-hadoop043");
        params.put(KuduConfig.KEY_TABLE_WHITELIST, "inventory.orders");
        // params.put(KuduConfig.KEY_TABLE_WHITELIST,
        // "seewo_easi_pass_ep_account_config,seewo_easi_pass_ep_offline_chanel,seewo_easi_pass_ep_offline_chanel_sta,seewo_easi_pass_ep_offline_channel_re_train,seewo_easi_pass_ep_im_group_classroom,seewo_easi_pass_ep_user_group_relation,seewo_easi_pass_ep_order,seewo_easi_pass_ep_class_transfer_record,seewo_easi_pass_ep_im_account,seewo_easi_pass_ep_user_join_class_record");
        params.put(KuduConfig.KEY_QUERY_TIMEOUT, "6000");
        params.put(KuduConfig.KEY_OPERATION_TIMEOUT, "6000");
        params.put(KuduConfig.KEY_ADMIN_OPERATION_TIMEOUT, "6000");
        params.put(KuduConfig.KEY_BATCH_SIZE, "10000");
        params.put(KuduSinkConfig.KEY_FLUSH_MODE, "auto_flush_sync");

        return new KuduSinkConfig(params);
    }

}
