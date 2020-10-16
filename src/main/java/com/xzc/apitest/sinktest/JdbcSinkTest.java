package com.xzc.apitest.sinktest;

import com.xzc.apitest.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Administrator
 */
public class JdbcSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> outputStreamOperator = dataStreamSource.map(value -> {
            String[] valArray = value.split(",");
            SensorReading sensorReading = new SensorReading(valArray[0], Long.valueOf(valArray[1]), Double.valueOf(valArray[2]));
            return sensorReading;
        });

        outputStreamOperator.addSink(new RichSinkFunction<SensorReading>() {
            private Connection connection;
            private PreparedStatement insert;
            private PreparedStatement update;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8", "root", "123456");
                insert = connection.prepareStatement("insert into sersor_temp(id, temp) values(?, ?)");
                update = connection.prepareStatement("update sersor_temp set temp = ? where id = ?");
                // 开启
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                insert.close();
                update.close();
                connection.close();

                // 关闭
                super.close();
            }

            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                update.setDouble(1, value.getTemperature());
                update.setString(2, value.getId());
                update.execute();
                if (update.getUpdateCount() == 0) {
                    // 没有则新增
                    insert.setString(1, value.getId());
                    insert.setDouble(2, value.getTemperature());
                    insert.execute();
                }
            }
        });

        env.execute("mysql sink");
    }
}
