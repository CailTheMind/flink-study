package com.xzc.tabletest;

import com.xzc.apitest.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        String path = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);
        SingleOutputStreamOperator<SensorReading> outputStreamOperator = dataStreamSource.map(value -> {
            String[] valArray = value.split(",");

            SensorReading sensorReading = new SensorReading(valArray[0], Long.valueOf(valArray[1]), Double.valueOf(valArray[2]));
            return sensorReading;
        });

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(outputStreamOperator);
        table.select($("id"), $("temperature")).filter($("id").isEqual("sensor_1")).execute().print();

        tableEnv.createTemporaryView("dataTable", outputStreamOperator, $("id"), $("timestamp"), $("temperature"));
        tableEnv.sqlQuery("select * from dataTable where id = 'sensor_1'").execute().print();

        env.execute("table api exam");

    }
}
