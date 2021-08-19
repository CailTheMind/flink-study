package com.xzc.demo2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.collection.mutable.ListBuffer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xzc
 */
public class LoginFail {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LoginEvent> streamSource = env.fromCollection(Arrays.asList(
                new LoginEvent(1, "192.168.0.1", "fail", 1629250510L),
                new LoginEvent(1, "192.168.0.2", "fail", 1629250519L),
                new LoginEvent(1, "192.168.0.3", "fail", 1629250520L),
                new LoginEvent(2, "192.168.0.3", "fail", 1629250521L),
                new LoginEvent(2, "192.168.0.10", "success", 1629250521L)
                )
        );

        SingleOutputStreamOperator<LoginEvent> outputStreamOperator = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>noWatermarks().withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.getTime() * 1000));
        // 过滤出 fail数据
        outputStreamOperator.filter((FilterFunction<LoginEvent>) value -> value.getResult().equals("fail"))
                .keyBy((KeySelector<LoginEvent, Integer>) value -> value.getUserId())
                // 不需要预计算
                .process(new MatchFunction()).print();

        env.execute("2222");

    }

    static class MatchFunction extends KeyedProcessFunction<Integer, LoginEvent, LoginEvent> {

        private transient ListState<LoginEvent> listState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("loginEventState", LoginEvent.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginEvent> out) throws Exception {

            List<LoginEvent> list = new ArrayList<>();
            // 触发逻辑
            for (LoginEvent loginEvent : listState.get()) {
                list.add(loginEvent);
            }

            listState.clear();

            // 2秒内 登录失败 至少 2次
            if (list != null && list.size() > 1) {
                // 用户第一条记录
                out.collect(list.get(0));
            }

        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginEvent> out) throws Exception {
            listState.add(value);
            // 在当前时间 + 2秒，时间到 则触发 on timer
            ctx.timerService().registerEventTimeTimer(value.getTime() * 1000 + 2 * 1000);
        }
    }

    @Data
    @AllArgsConstructor
    static class LoginEvent {
        Integer userId;
        String ip;
        String result;
        Long time;
    }
}
