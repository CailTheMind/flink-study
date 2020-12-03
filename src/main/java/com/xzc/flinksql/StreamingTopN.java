package com.xzc.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

/**
 * @author Administrator
 */
public class StreamingTopN {

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
    StreamExecutionEnvironment streamEvn = StreamExecutionEnvironment.getExecutionEnvironment();
//    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEvn, settings);

    String sourceDDl = "create table source_kafka " +
            "(" +
            " app_id String," +
            " send_time String," +
            " serial_number String" +
            ") with " +
            "(" +
            " 'connector.type' = 'kafka', " +
//            " 'connector.version' = '0.10' " +
            " 'connector.properties.bootstrap.servers' = '101.200.43.65:9092,47.95.29.151:9092,101.200.216.136:9092'," +
            " 'connector.properties.zookeeper.connect' = '101.200.43.65:2181,47.95.29.151:2181,101.200.216.136:2181'," +
            " 'connector.topic' = 'regional_group_paper_input'," +
            " 'connector.properties.group.id' = 'canpoint'," +
            " 'connector.startup-mode' = 'latest-offset'," +
            " 'connector.auto.offset.reset' = 'latest'," +
            " 'format.type' = 'json'" +
            ")";


}
