package com.xzc.apitest.sinktest;

import com.xzc.apitest.SensorReading;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.nio.charset.Charset;
import java.util.Properties;

/**
 * @author xzc
 */
public class KafkaSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> outputStreamOperator = dataStreamSource.map(value -> {
            String[] valArray = value.split(",");
            SensorReading sensorReading = new SensorReading(valArray[0], Long.valueOf(valArray[1]), Double.valueOf(valArray[2]));
            return sensorReading;
        });

        Properties properties = new Properties();
        //kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
        properties.setProperty("zookeeper.connect", "localhost:2181");
        //flink consumer flink的消费者的group.id
        properties.setProperty("group.id", "test");

        env.addSource(new FlinkKafkaConsumer<String>("sersor-test", new SimpleStringSchema(), properties)).print();

        outputStreamOperator.addSink(new FlinkKafkaProducer<SensorReading>("sersor-test", new SerializationSchema<SensorReading>() {
            @Override
            public byte[] serialize(SensorReading element) {
                // 转换成自己想要的格式 比如  直接实体类toString 或者 封装成string  或者封装成 hash转 toString
                return element.toString().getBytes();
            }
        }, properties));

        env.execute("sersor sink");


    }
}
