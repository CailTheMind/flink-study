package com.xzc.flinksql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

import java.util.Properties;

public class Demo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        Properties props = new Properties();
        props.put("bootstrap.servers", "");
        props.put("zookeeper.connect", "");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        props.put("group.id", "canpoint");
        props.put("group", "regional");

        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer("regional_group_paper_input", new SimpleStringSchema(), props);
//        SingleOutputStreamOperator<DataInfo<GroupPaperInputDTO>> streamOperator = bsEnv.addSource(myConsumer).map((MapFunction<String, DataInfo<GroupPaperInputDTO>>) value -> {
//            try {
//
//                DataInfo<GroupPaperInputDTO> result = new Gson().fromJson(value, new TypeToken<DataInfo<GroupPaperInputDTO>>() {
//                }.getType());
//                return result;
//            } catch (Exception e) {
//                return null;
//            }
//        }).returns(TypeInformation.of(DataInfo.class)).filter((FilterFunction) value -> value != null);

//        SingleOutputStreamOperator<Tuple3<String, String, Long>> map = streamOperator.map(new MapFunction<DataInfo<GroupPaperInputDTO>, Tuple3<String, String, Long>>() {
//            @Override
//            public Tuple3<String, String, Long> map(DataInfo<GroupPaperInputDTO> value) throws Exception {
//                return Tuple3.of(value.getApp_id(), value.getSerial_number(), value.getSend_time());
//            }
//        });
//        Table table = bsTableEnv.fromDataStream(map, $("f0").as("appId"), $("f1").as("serialNumber"), $("f2").as("sendTime"));
//        bsTableEnv.createTemporaryView("groupPaper", map, $("f0").as("appId"), $("f1").as("serialNumber"), $("f2").as("sendTime"));
//        table.orderBy($("sendTime").desc()).select($("serialNumber"), $("sendTime")).
//
//        String topSql = "insert into sink_print SELECT * " +
//                "FROM (" +
//                "   SELECT appId, sendTime," +
//                "       ROW_NUMBER() OVER (PARTITION BY appId, sendTime ORDER BY sendTime DESC) as row_num" +
//                "   FROM groupPaper ) " +
//                "WHERE row_num <= 2";
//
//        String PRINT_SINK_SQL = "create VIEW sink_print AS " +
//                " SELECT *" +
//                "FROM (" +
//                "  SELECT appId,sendTime," +
//                "    ROW_NUMBER() OVER (PARTITION BY appId ORDER BY sendTime DESC) AS rownum " +
//                "  FROM groupPaper) " +
//                "WHERE rownum <= 2";
//
//        bsTableEnv.executeSql(PRINT_SINK_SQL);
////        bsTableEnv.executeSql(topSql);
//        bsTableEnv.toRetractStream(bsTableEnv.sqlQuery("select * from sink_print"), Row.class).filter(new FilterFunction<Tuple2<Boolean, Row>>() {
//            @Override
//            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
//                return value.f0;
//            }
//        }).print();
//        String execSQL = ""
//                + "SELECT *  "
//                + "FROM ( "
//                + "  SELECT appId, sendTime, "
//                + "     ROW_NUMBER() OVER (PARTITION BY appId, sendTime ORDER BY sendTime DESC) AS rowNum "
//                + "  FROM groupPaper "
//                + ") t "
//                + "WHERE rowNum <= 2";
//        String execSQL = "SELECT appId, sendTime FROM groupPaper ORDER BY sendTime DESC LIMIT 2";
//        Table resultTable = bsTableEnv.sqlQuery(execSQL);
//        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = bsTableEnv.toRetractStream(resultTable, Row.class);
//        tuple2DataStream.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
//            @Override
//            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
//                return value.f0;
//            }
//        }).print();

        bsEnv.execute("flink SQL");
    }
}
