package com.xzc.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import lombok.val;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.hbase.source.HBaseTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

public class Test1 {

    public static void main(String[] args) throws Exception {
        // 创建Blink Streaming的TableEnvironment
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);

        // 创建表，connector使用mysql-cdc
        bsTableEnv.executeSql("CREATE TABLE mysql_binlog " +
                "(id STRING, " +
                "times STRING, " +
                "temp DOUBLE) " +
                "WITH " +
                "('connector' = 'mysql-cdc', " +
                " 'hostname' = '127.0.0.1', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'test', " +
                " 'table-name' = 'sersor_temp'" +
                ")"
        );

        // 创建下游数据表，这里使用print类型的connector，将数据直接打印出来
//        bsTableEnv.executeSql("CREATE TABLE sink_table " +
//                "(id STRING, " +
//                "times STRING, " +
//                "temp DOUBLE) " +
//                "WITH " +
//                "('connector' = 'print'" +
//                ")"
//        );
        // 将CDC数据源和下游数据表对接起来
//        bsTableEnv.executeSql("INSERT INTO sink_table SELECT id, times, temp FROM mysql_binlog");

        // 将数据 发送到kafka
        bsTableEnv.executeSql("CREATE TABLE sink_kafka_table " +
                "(id STRING, " +
                "times STRING, " +
                "temp DOUBLE " +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'test_mysql_binlog'," +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'properties.group.id' = 'testGroup'," +
                " 'properties.bootstrap.servers' = 'node2:9092', " +
                " 'format' = 'canal-json' " +
                ")"
        );
        // 将CDC数据与 kafka表对接起来
        bsTableEnv.executeSql("INSERT INTO sink_kafka_table SELECT id, times, temp FROM mysql_binlog");

        bsTableEnv.executeSql("CREATE TABLE hTable (" +
                " id STRING," +
                " f ROW<times STRING, temp STRING>," +
                " PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name' = 'regional:binlog'," +
                " 'zookeeper.quorum' = 'node2:2181'" +
                ")");
        // 将CDC数据存储到 Hbase中
        bsTableEnv.executeSql("INSERT INTO hTable SELECT id, ROW(times, temp) FROM mysql_binlog");
    }
}
