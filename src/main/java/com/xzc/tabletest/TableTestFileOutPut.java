package com.xzc.tabletest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableTestFileOutPut {

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

        // 过滤
        Table filter = streamTableEnvironment.sqlQuery("select id, temperature from MyUserTable").filter($("id").isEqual("sensor_1"));
        Table filter2 = streamTableEnvironment.sqlQuery("select id, temperature from MyUserTable where id = 'sensor_1'");

        // 聚合统计
        Table select = inputTable.groupBy($("id")).select($("id"), $("temperature").sum().as("sum"));
        Table groupTable = streamTableEnvironment.sqlQuery("select id, sum(temperature) as sumTemp from MyUserTable group by id");


        // 连接外部文件，注册输出表
        String outPutSql = "CREATE TABLE outputTable (" +
                "  id STRING," +
                "  temperature Double " +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'E:\\IdeaProjects\\flink-study\\src\\main\\resources\\out.txt',  " +
                "  'format' = 'csv'," +
                "  'partition.default-name' = 'test' " +
                ")";
        TableResult tableResult = streamTableEnvironment.executeSql(outPutSql);
        streamTableEnvironment.from("outputTable");
        // 输入到文件 不支持 聚合操作 使用会报错
        streamTableEnvironment.createStatementSet()
                .addInsertSql("insert into outputTable select id, temperature from MyUserTable where id = 'sensor_1' ").execute();

    }
}
