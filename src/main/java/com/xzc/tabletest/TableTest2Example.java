package com.xzc.tabletest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest2Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSetting);

        String slq = "CREATE TABLE MyUserTable (" +
                "  id STRING," +
                "  times String, " +
                "  temperature Double " +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'file:///E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt',  " +
                "  'format' = 'csv'," +
                "  'partition.default-name' = 'test' " +
                ")";

        streamTableEnvironment.executeSql(slq);
        Table inputTable = streamTableEnvironment.from("MyUserTable");
        streamTableEnvironment.toAppendStream(inputTable, Row.class).print("全表");
        //        streamTableEnvironment.connect(new FileSystem().path("E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt"))
//                .withFormat(new Csv())
//                .withSchema(new Schema().field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
//                        .field("temperature", DataTypes.DOUBLE()))
//                .createTemporaryTable("csvTable");
//        Table csvTable = streamTableEnvironment.from("csvTable");
//        streamTableEnvironment.toAppendStream(csvTable, Row.class).print();
        // 过滤
        Table filter = streamTableEnvironment.sqlQuery("select id, temperature from MyUserTable").filter($("id").isEqual("sensor_1"));
        streamTableEnvironment.toAppendStream(filter, Row.class).print("过滤1");
        Table filter2 = streamTableEnvironment.sqlQuery("select id, temperature from MyUserTable where id = 'sensor_1'");
        streamTableEnvironment.toAppendStream(filter2, Row.class).print("过滤2");
        // 聚合统计
        Table select = inputTable.groupBy($("id")).select($("id"), $("temperature").sum().as("sum"));
        streamTableEnvironment.toRetractStream(select, Row.class).print("聚合统计11");
        Table groupTable = streamTableEnvironment.sqlQuery("select id, sum(temperature) from MyUserTable group by id");
        streamTableEnvironment.toRetractStream(groupTable, Row.class).print("聚合统计222");

        // 窗口操作

//        streamTableEnvironment.createTemporaryView("sersor", inputTable);
//        Table select1 = inputTable.window(Over.partitionBy("id").orderBy($("rt")).preceding($("2.rows")).as("ow"))
//                .select($("id"), $("rw"), $("id").count().over("ow"), $("tmpt").avg().over("ow"));
//        streamTableEnvironment.toRetractStream(select1, Row.class).print("tableAPI");

        env.execute("11");
    }
}
