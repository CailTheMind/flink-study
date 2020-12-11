package com.xzc.tabletest;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableTestExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 老版本流处理
        EnvironmentSettings oldStreamSetting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSetting);

        // 老版本 planner 批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 基于 blink 批处理
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSetting);

        // 基于 blink 流处理
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSetting);


    }
}
