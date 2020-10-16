package com.xzc.apitest;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author xzc
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从集合中读取数据
//        List<SensorReading> sensorReadingList = new ArrayList<SensorReading>();
//        sensorReadingList.add(new SensorReading("sensor_1", 1547718199L, 35.2));
//        sensorReadingList.add(new SensorReading("sensor_2", 1547718598L, 33.1));
//        sensorReadingList.add(new SensorReading("sensor_3", 1547718456L, 32.4));
//        sensorReadingList.add(new SensorReading("sensor_4", 1547718685L, 31.5));
//        sensorReadingList.add(new SensorReading("sensor_5", 1547718325L, 30.6));
//        sensorReadingList.add(new SensorReading("sensor_6", 1547718128L, 21.7));
//
//        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.fromCollection(sensorReadingList);
//
//        env.fromElements(1, 35, "hello").map(new MapFunction<Serializable, Object>() {
//
//        });

//        sensorReadingDataStreamSource.print();

        // 从kafka
//        Properties properties = new Properties();
//        //kafka的节点的IP或者hostName，多个使用逗号分隔
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        //zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
//        properties.setProperty("zookeeper.connect", "localhost:2181");
//        //flink consumer flink的消费者的group.id
//        properties.setProperty("group.id", "test");
//        env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties)).print();

        // 从自定义数据源读取
        env.addSource(new SourceFunction<SensorReading>() {
            // 数据源是否运行标识
            boolean running = true;

            @Override
            public void run(SourceContext<SensorReading> ctx) throws Exception {
                List<SensorReading> sensorReadingList = new ArrayList<>(10);
                // 准备10条数据
                for (int i = 0; i < 10; i++) {

                    sensorReadingList.add(new SensorReading("sensor_" + i, System.currentTimeMillis(),  RandomUtils.nextDouble(1, 100)));
                }
                while (running) {
                    sensorReadingList.forEach(e -> ctx.collect(e));
                    TimeUnit.SECONDS.sleep(5);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).print();


        env.execute("source test");
    }
}
