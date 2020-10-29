package com.xzc.apitest;

import org.apache.flink.addons.hbase.HBaseUpsertSinkFunction;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WindowTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        String path = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\sersor.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> outputStreamOperator = dataStreamSource.map(value -> {
            String[] valArray = value.split(",");
            SensorReading sensorReading = new SensorReading(valArray[0], Long.valueOf(valArray[1]), Double.valueOf(valArray[2]));
            return sensorReading;
        })
//        .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))); // 延迟3秒的固定水印
//        .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()); // 单调递增水印
//          .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())); // 从元素element中提取我们想要的eventtime
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() *1000;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> mapStreamOperator = outputStreamOperator.map((MapFunction<SensorReading, Tuple2<String, Double>>) value -> new Tuple2<>(value.getId(), value.getTemperature()));

        SingleOutputStreamOperator<Tuple2<String, Double>> window = mapStreamOperator.keyBy(new KeySelector<Tuple2<String, Double>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Double> value) throws Exception {
                return value.f0;
            }
//        }).window(TumblingEventTimeWindows.of(Time.seconds(15))); // 滚动窗口 偏移量 在时区时使用，默认伦敦时间，比如 按北京时间来算的时候，要-8个小时
//        }).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10))); // 滑动窗口 第二个参数是 滑动距离
//        }).window(EventTimeSessionWindows.withGap(Time.seconds(10)));// 会话窗口 以时间为分隔点，小于等于这个时间为一个窗口，大于这个时间为另外一个窗口
//          }).countWindow(10); // 滚动计数窗口
        }).timeWindow(Time.seconds(10))
//                .minBy(1);  // 安装温度分组统计
        .reduce(new ReduceFunction<Tuple2<String, Double>>() {
            // 获取最小温度值
            @Override
            public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
                Tuple2<String, Double> tuple2 = new Tuple2<>();
                tuple2.f0 = value1.f0;
                tuple2.f1 = value1.f1 < value2.f1 ? value1.f1 : value2.f1;
                return tuple2;
            }
        });

        HBaseSinkFunction sinkFunction = new HBaseSinkFunction();
        sinkFunction.invoke(new Tuple2<String, String>(), new SinkFunction.Context() {
            @Override
            public long currentProcessingTime() {
                return 0;
            }

            @Override
            public long currentWatermark() {
                return 0;
            }

            @Override
            public Long timestamp() {
                return null;
            }
        });
        sinkFunction.close();
        sinkFunction.open(new Configuration());

        // 建议流式 速度较快
        // 窗口流式处理 aggregate 代替 fold
//        window.aggregate(new AggregateFunction<Tuple2<String, Double>, Object, Object>() {
//            @Override
//            public Object createAccumulator() {
//                return null;
//            }
//
//            @Override
//            public Object add(Tuple2<String, Double> value, Object accumulator) {
//                return null;
//            }
//
//            @Override
//            public Object getResult(Object accumulator) {
//                return null;
//            }
//
//            @Override
//            public Object merge(Object a, Object b) {
//                return null;
//            }
//        });
        // 窗口流式处理
//        window.reduce(new ReduceFunction<Tuple2<String, Double>>() {
//            @Override
//            public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
//                return null;
//            }
//        });

        // 窗口批量处理
//        window.process(new ProcessWindowFunction<Tuple2<String, Double>, Object, Object, TimeWindow>() {
//            @Override
//            public void process(Object o, Context context, Iterable<Tuple2<String, Double>> elements, Collector<Object> out) throws Exception {
//
//            }
//        });

        // 窗口批量处理
//        window.apply(new WindowFunction<Tuple2<String, Double>, Object, Object, TimeWindow>() {
//            @Override
//            public void apply(Object o, TimeWindow window, Iterable<Tuple2<String, Double>> input, Collector<Object> out) throws Exception {
//
//            }
//        });

        // 窗口触发器 定义window什么时候关闭，触发计算结果并输出结果
//        window.trigger(new Trigger<Tuple2<String, Double>, TimeWindow>() {
//            @Override
//            public TriggerResult onElement(Tuple2<String, Double> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//                return null;
//            }
//
//            @Override
//            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                return null;
//            }
//
//            @Override
//            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                return null;
//            }
//
//            @Override
//            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//
//            }
//        });

        // 窗口移除器 定义移除某些数据的逻辑
//        window.evictor(new Evictor<Tuple2<String, Double>, TimeWindow>() {
//            @Override
//            public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Double>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
//
//            }
//
//            @Override
//            public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Double>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
//
//            }
//        });

        // 允许处理迟到的数据
//        window.allowedLateness(Time.seconds(10));

        // 将迟到的数据放入 侧输出流
//        OutputTag<Tuple2<String,Double>> high = new OutputTag<Object>("high", TypeInformation.of(Tuple2.class));
//        window.sideOutputLateData(high);

        // 不建议使用 windowall 会把所有数据放入一个窗口，并行度会变成1

        // 获取迟到数据
//        mapStreamOperator.getSideOutput(high);
    }
}
