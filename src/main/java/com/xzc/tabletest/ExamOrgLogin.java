package com.xzc.tabletest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ExamOrgLogin {
    public static void main(String[] args) {
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
                " 'properties.bootstrap.servers' = ''," +
                " 'properties.group.id' = 'testGroup'," +
                " 'format' = 'csv'," +
                " 'scan.startup.mode' = 'earliest-offset'" +
                ")";
        streamTableEnvironment.executeSql(connection);

    }

}
