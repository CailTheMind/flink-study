package com.xzc.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Demo4 {

    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String ddl = " create table school_login_count" +
                "( id varchar(30), " +
                " `date` BIGINT, " +
                " regional_id BIGINT, " +
                " school_id BIGINT, " +
                " `count` BIGINT, " +
                " PRIMARY KEY (id) NOT ENFORCED " +
                " ) with ( " +
                " 'connector' = 'jdbc', " +
                " 'url' = '', " +
                " 'username' = '', " +
                " 'password' = '', " +
                " 'table-name' = 'regional_school_login_count_copy' )";

        bsTableEnv.executeSql(ddl);



    }
}
