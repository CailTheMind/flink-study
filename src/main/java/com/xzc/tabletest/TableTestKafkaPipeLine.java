package com.xzc.tabletest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableTestKafkaPipeLine {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSetting);

        String connection = "CREATE TABLE kafkaTable (" +
                "  id STRING," +
                "  times String, " +
                "  temperature Double " +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'test'," +
                " 'properties.bootstrap.servers' = '101.200.43.65:9092,47.95.29.151:9092,101.200.216.136:9092'," +
                " 'properties.group.id' = 'testGroup'," +
                " 'format' = 'csv'," +
                " 'scan.startup.mode' = 'earliest-offset'" +
                ")";
        streamTableEnvironment.executeSql(connection);

        Table kafkaTable = streamTableEnvironment.from("kafkaTable");

        Table table = streamTableEnvironment.sqlQuery("select * from kafkaTable");
        streamTableEnvironment.toAppendStream(table, Row.class).print();

        Table filter = streamTableEnvironment.sqlQuery("select id, temperature from kafkaTable").filter($("id").isEqual("sensor_1"));
        streamTableEnvironment.toAppendStream(filter, Row.class).print("过滤1");
        Table filter2 = streamTableEnvironment.sqlQuery("select id, temperature from kafkaTable where id = 'sensor_1'");
        streamTableEnvironment.toAppendStream(filter2, Row.class).print("过滤2");
        // 聚合统计
        Table select = kafkaTable.groupBy($("id")).select($("id"), $("temperature").sum().as("sum"));
        streamTableEnvironment.toRetractStream(select, Row.class).print("聚合统计11");
        Table groupTable = streamTableEnvironment.sqlQuery("select id, sum(temperature) from kafkaTable group by id");
        streamTableEnvironment.toRetractStream(groupTable, Row.class).print("聚合统计222");

        env.execute("kafka table");
    }
}
