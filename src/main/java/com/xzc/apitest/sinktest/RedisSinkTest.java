package com.xzc.apitest.sinktest;

import com.xzc.apitest.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xzc
 */
public class RedisSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> outputStreamOperator = dataStreamSource.map(value -> {
            String[] valArray = value.split(",");
            SensorReading sensorReading = new SensorReading(valArray[0], Long.valueOf(valArray[1]), Double.valueOf(valArray[2]));
            return sensorReading;
        });

//        outputStreamOperator.addSink(new RedisSink<>(new FlinkJedisPoolConfig.Builder()
//                .setHost("localhost").setPort(6379).setPassword("123456").build(), new RedisMapper<SensorReading>() {
//            /**
//             * 定义 redis保存命令
//             *
//             * @return
//             */
//            @Override
//            public RedisCommandDescription getCommandDescription() {
//                return new RedisCommandDescription(RedisCommand.HSET, "sersor_temp");
//            }
//
//            /**
//             * redis key
//             *
//             * @param sensorReading
//             * @return
//             */
//            @Override
//            public String getKeyFromData(SensorReading sensorReading) {
//                return sensorReading.getId();
//            }
//
//            /**
//             * redis value
//             *
//             * @param sensorReading
//             * @return
//             */
//            @Override
//            public String getValueFromData(SensorReading sensorReading) {
//                return sensorReading.getTemperature().toString();
//            }
//        }));
        env.execute("redis test");
    }
}
