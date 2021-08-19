package com.xzc.demo2;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Tomcat 请求日志分析 统计请求次数前几名的地址
 * @author xzc
 */
public class TomcatAccessLogAnalyze {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\IdeaProjects\\flink-study\\src\\main\\resources\\localhost_access_log.2021-08-18.txt");

        // 清洗数据
        SingleOutputStreamOperator<RequestAccessLog> outputStreamOperator = dataStreamSource.map(new MapFunction<String, RequestAccessLog>() {
            @Override
            public RequestAccessLog map(String value) {
                try {
                    String[] split = value.split(" ");
                    String ip = split[0];
                    String requestTime = value.substring(value.indexOf("[") + 1, value.indexOf("]"));
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
                    String requestInfo = value.substring(value.indexOf("\"") + 1, StringUtils.lastIndexOf(value, "\""));
                    String[] requestSplit = requestInfo.split(" ");
                    String requestType = requestSplit[0];
                    String requestUrl = requestSplit[1];
                    if (requestUrl.contains("?")) {
                        requestUrl = requestUrl.substring(0, requestUrl.indexOf("?"));
                    }
                    return new RequestAccessLog(ip, sdf.parse(requestTime).getTime(), requestType, requestUrl, Integer.valueOf(split[split.length - 1]));
                } catch (Exception e) {
                    return null;
                }
            }
        }).filter(e -> e != null);

        // 只要数据出现乱序 必须使用水印
        SingleOutputStreamOperator<RequestAccessLog> streamOperator = outputStreamOperator
                .assignTimestampsAndWatermarks(WatermarkStrategy.<RequestAccessLog>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<RequestAccessLog>) (requestAccessLog, l) -> requestAccessLog.getTime()));

        // 根据URL分组，由于是统计 URL 请求次数
        KeyedStream<RequestAccessLog, String> keyedStream = streamOperator.keyBy((KeySelector<RequestAccessLog, String>) behaviorData -> behaviorData.getUrl());
        // 开时间窗口 每5秒钟 统计1分钟内的数据 根据URL请求次数求和
        SingleOutputStreamOperator<AccessLogCount> aggregate = keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(5), Time.seconds(5))).aggregate(new CountAgg(), new WindowResultFunction());
        // 对 窗口做分组 然后处理每个窗口的数据
        aggregate.keyBy((KeySelector<AccessLogCount, Long>) logCount -> logCount.getEndTime()).process(new TopNProcess(10)).print();

        env.execute("1111");

    }

    static class TopNProcess extends KeyedProcessFunction<Long, AccessLogCount, String> {

        int size = 0;

        // 状态存储
        private transient ListState<AccessLogCount> listState = null;

        public TopNProcess(int size) {
            this.size = size;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态的注册
            ListStateDescriptor<AccessLogCount> itemsState = new ListStateDescriptor<>(
                    "userState",
                    AccessLogCount.class);
            listState = getRuntimeContext().getListState(itemsState);
        }

        @Override
        public void processElement(AccessLogCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);

            // 触发定时器  加的时间为 水印时间
            ctx.timerService().registerEventTimeTimer(value.getEndTime() + 3 * 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            List<AccessLogCount> list = new ArrayList<>(size);

            for (AccessLogCount logCount : listState.get()) {
                list.add(logCount);
            }
            listState.clear();

            // 排序 并截取
            List<AccessLogCount> behaviorDataCountList = list.stream().sorted(Comparator.comparing(AccessLogCount::getCount).reversed()).collect(Collectors.toList());


            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("开始时间: ").append(new Timestamp(timestamp - 5 * 1000)).append("\n");
            for (AccessLogCount logCount : behaviorDataCountList) {
                result.append(logCount.getUrl()).append("  ").append(logCount.getCount()).append("\n");
            }
            result.append("结束时间: ").append(new Timestamp(timestamp)).append("\n").append("====================================\n\n");

            out.collect(result.toString());
        }
    }

    static class WindowResultFunction implements WindowFunction<Long, AccessLogCount, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AccessLogCount> out) throws Exception {
            out.collect(new AccessLogCount(s, window.getEnd(), input.iterator().next()));
        }
    }

    static class CountAgg implements AggregateFunction<RequestAccessLog, Long, Long> {

        @Override
        public Long createAccumulator() {
            // 初始值
            return 0L;
        }

        @Override
        public Long add(RequestAccessLog log, Long aLong) {
            // 一个一个往上加
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            // 结算结果
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            // keyBy时 2个结果放一起
            return aLong + acc1;
        }
    }

    @Data
    @AllArgsConstructor
    static class AccessLogCount{
        String url;
        Long endTime;
        Long count;
    }

    @Data
    @AllArgsConstructor
    static class RequestAccessLog {
        String ip;
        Long time;
        String type;
        String url;
        /**
         * 毫秒
         */
        Integer consumeTime;
    }
}
