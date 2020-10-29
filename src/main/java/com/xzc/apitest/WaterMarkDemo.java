package com.xzc.apitest;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //设置周期性的产生水位线的时间间隔。当数据流很大的时候，如果每个事件都产生水位线，会影响性能。
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 7979);
        stream.flatMap(new FlatMapFunction<String, StationLog>() {
            @Override
            public void flatMap(String value, Collector<StationLog> out) throws Exception {
                String[] words = value.split(",");
                out.collect(new StationLog(words[0], words[1], words[2], Long.valueOf(words[3]), Long.valueOf(words[4])));
            }
        }).filter(new FilterFunction<StationLog>() {
            @Override
            public boolean filter(StationLog value) throws Exception {
                return value.getDuration() > 0;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<StationLog>() {
            @Override
            public long extractTimestamp(StationLog element, long recordTimestamp) {
                //指定EventTime对应的字段
                return element.getCallTime();
            }
        })).keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog value) throws Exception {
                return value.getStationID();
            }
        }).timeWindow(Time.seconds(5)) // 时间窗口
        .reduce(new ReduceFunction<StationLog>() {
            @Override
            public StationLog reduce(StationLog value1, StationLog value2) throws Exception {
                // 找到通话时间最长的通话记录
                return value1.getDuration() >= value2.getDuration() ? value1 : value2;
            }
        }, new ProcessWindowFunction<StationLog, Object, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<StationLog> elements, Collector<Object> out) throws Exception {
                // 窗口处理完成后，输出的结果是什么
                StationLog maxLog = elements.iterator().next();

                StringBuffer sb = new StringBuffer();
                sb.append("窗口范围是:").append(context.window().getStart()).append("----").append(context.window().getEnd()).append("\n");;
                sb.append("基站ID：").append(maxLog.getStationID()).append("\t")
                        .append("呼叫时间：").append(maxLog.getCallTime()).append("\t")
                        .append("主叫号码：").append(maxLog.getFrom()).append("\t")
                        .append("被叫号码：")  .append(maxLog.getTo()).append("\t")
                        .append("通话时长：").append(maxLog.getDuration()).append("\n");
                out.collect(sb.toString());
            }
        }).print();

        env.execute("water demo");
    }
}
