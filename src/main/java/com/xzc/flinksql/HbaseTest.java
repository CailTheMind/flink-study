package com.xzc.flinksql;

import org.apache.flink.connector.hbase.source.HBaseTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;

public class HbaseTest {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEvn = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEvn, settings);

        Configuration conf = new Configuration();
        conf.set("", "");
        HBaseTableSource hSrc = new HBaseTableSource(conf, "hTable");
        hSrc.addColumn("fam1", "col1", byte[].class);
        hSrc.addColumn("fam1", "col2", Integer.class);
        hSrc.addColumn("fam2", "col1", String.class);

//        tableEnv.registerTableSource("hTable", hSrc);

//        Table res = tableEnv.sqlQuery("SELECT t.fam2.col1, SUM(t.fam1.col2) FROM hTable AS t GROUP BY t.fam2.col1");
    }
}
