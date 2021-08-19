package com.xzc.demo1;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 开窗
 */
public class WindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("document", "test")
                .username("root")
                .password("123456")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();


        env.addSource(sourceFunction).print(">>").setParallelism(1);

        env.execute("121");
    }
}
