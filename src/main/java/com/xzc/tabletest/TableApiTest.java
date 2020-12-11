package com.xzc.tabletest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建表执行环境
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        String ddlSql = " create table mysqlSourceTable " +
                "( id bigint, " +
                " org_id bigint, " +
                " status tinyint, " +
                " time_start bigint, " +
                " time_end bigint, " +
                " time_create bigint, " +
                " time_update bigint, " +
                " note varchar(100) " +
                " ) with ( " +
                " 'connector' = 'jdbc', " +
                " 'url' = 'jdbc:mysql://39.105.153.115:3306/regional_bigdata?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true&failOverReadOnly=false', " +
                " 'username' = 'root', " +
                " 'password' = 'cpcloud2020', " +
                " 'driver' = 'com.mysql.cj.jdbc.Driver', " +
                " 'table-name' = 'opened_org' )";
        tableEnv.executeSql(ddlSql);
        Table mysqlSourceTable = tableEnv.from("mysqlSourceTable");
        Table select = mysqlSourceTable.select($("id"), $("org_id"));

        Table table = tableEnv.sqlQuery("select * from mysqlSourceTable");

        tableEnv.toAppendStream(select, Row.class).print("result");
        tableEnv.toAppendStream(table, Row.class).print("sql");
        env.execute("table api exam");
    }
}
