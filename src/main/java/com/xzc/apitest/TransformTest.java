package com.xzc.apitest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class TransformTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setParallelism(1);
        String path = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> outputStreamOperator = dataStreamSource.map(value -> {
            String[] valArray = value.split(",");
            SensorReading sensorReading = new SensorReading(valArray[0], Long.valueOf(valArray[1]), Double.valueOf(valArray[2]));
            return sensorReading;
        });

        // 分组
//        outputStreamOperator.keyBy((KeySelector<SensorReading, Object>) value -> value.getId()).minBy("temperature").print();

        // 最大温度的 最近时间
//        outputStreamOperator.keyBy((KeySelector<SensorReading, Object>) value -> value.getId()).reduce((ReduceFunction<SensorReading>) (value1, value2) -> {
//            SensorReading newSensorReading = new SensorReading();
//            newSensorReading.setId(value1.getId());
//            // 最大值
//            newSensorReading.setTemperature(value1.getTemperature() > value2.getTemperature()? value1.getTemperature() : value2.getTemperature());
//            // 最近时间
//            newSensorReading.setTimestamp(value2.getTimestamp());
//            return newSensorReading;
//        }).print();

        // 以30度为分界点 低于30 为低温 高于30位高温 （已过时）
//        SplitStream<SensorReading> split = outputStreamOperator.split(new OutputSelector<SensorReading>() {
//            @Override
//            public Iterable<String> select(SensorReading value) {
//                List<String> output = new ArrayList<>();
//                if (value.getTemperature() > 30.0) {
//                    output.add("high");
//                } else {
//                    output.add("low");
//                }
//                return output;
//            }
//        });
//        DataStream<SensorReading> high = split.select("high");
//        DataStream<SensorReading> low = split.select("low");
//        DataStream<SensorReading> all = split.select("high", "low");
//        high.print("high");
//        low.print("low");
//        all.print("all");

        // 使用Side Output分流
        final OutputTag<SensorReading> high = new OutputTag<>("high", TypeInformation.of(SensorReading.class));
        final OutputTag<SensorReading> low = new OutputTag<>("low", TypeInformation.of(SensorReading.class));
        SingleOutputStreamOperator<Object> process = outputStreamOperator.process(new ProcessFunction<SensorReading, Object>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<Object> out) throws Exception {
                if (value.getTemperature() > 30.0) {
                    ctx.output(high, value);
                } else {
                    ctx.output(low, value);
                }
                out.collect(value);
            }
        });
        DataStream<SensorReading> highDataStream = process.getSideOutput(high);
        DataStream<SensorReading> lowDataStream = process.getSideOutput(low);
//        highDataStream.print("high");
//        lowDataStream.print("low");
//        highDataStream.union(lowDataStream).print("all");

        // 合流 connect 场景：多条件控制
//        ConnectedStreams<SensorReading, SensorReading> connectedStreams = highDataStream.map(new MapFunction<SensorReading, SensorReading>() {
//            @Override
//            public SensorReading map(SensorReading value) throws Exception {
//                value.setTimestamp(null);
//                return value;
//            }
//        }).connect(lowDataStream);
//        connectedStreams.map(new CoMapFunction<SensorReading, SensorReading, Object>() {
//
//            @Override
//            public Object map1(SensorReading value) throws Exception {
//                // 高温处理 数据格式可以和 低温不同
//                return value;
//            }
//            @Override
//            public Object map2(SensorReading value) throws Exception {
//                // 低温 处理 数据格式可以和 高温不同
//                List<String> resultList = new ArrayList<>(2);
//                resultList.add(value.getId());
//                resultList.add("bilibili");
//                return resultList;
//            }
//        }).print("connect map");

        // 和 MapFunction区别： 可以获得运行上下文
//        new RichMapFunction<SensorReading, String>() {
//            // 快捷键 ctrl+o
//            @Override
//            public String map(SensorReading value) throws Exception {
//                return value.getId();
//            }
//
//            /**
//             * 在类构造方法之后，加入flink执行环境中，此时没有数据 调用open
//             * @param parameters
//             * @throws Exception
//             */
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                // 做一些初始化操作，比如 初始化数据库连接
//                super.open(parameters);
//            }
//
//            @Override
//            public void close() throws Exception {
//                // 做一些收尾工作，比如 连接关闭，状态修改
//                super.close();
//            }
//        };
//        new MapFunction<SensorReading, String>(){
//
//            @Override
//            public String map(SensorReading value) throws Exception {
//                return value.getId();
//            }
//        };

        // 直接把结果输出到文件
        lowDataStream.addSink(StreamingFileSink.forRowFormat(new Path("输出到文件路径"), new SimpleStringEncoder<SensorReading>()).build());

        env.execute("transform test");
    }
}
