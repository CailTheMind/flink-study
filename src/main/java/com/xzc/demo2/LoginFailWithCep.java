package com.xzc.demo2;

import com.ibm.icu.impl.TimeZoneGenericNames;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.cep.pattern.Pattern;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author xzc
 */
public class LoginFailWithCep {

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
        // 时间循序 则不需要水印
        SingleOutputStreamOperator<LoginEvent> outputStreamOperator = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>noWatermarks().withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.getTime() * 1000));
        // 过滤出 fail数据
        outputStreamOperator.filter((FilterFunction<LoginEvent>) value -> value.getResult().equals("fail"))
                .keyBy((KeySelector<LoginEvent, Integer>) value -> value.getUserId());

        // 下面一块代码为Java实现  Scala 和 Java 不一样
        // 定义匹配规则
        // begin 方法指第一次出现， next方法指 紧接着即第二次出现  就是连续2次出现
        // times 指上面现象出现1次 在 within 时间内
        // 即 在时间范围内 连续出现1次
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<LoginEvent, ?> loginFail = Pattern.<LoginEvent>begin("begin", skipStrategy).where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return value.getResult().equals("fail");
            }
        }).next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return value.getResult().equals("fail");
            }
        }).times(1).within(Time.seconds(2));

        // 模式流
        PatternStream<LoginEvent> patternStream = CEP.pattern(outputStreamOperator, loginFail);

        patternStream.select((PatternSelectFunction<LoginEvent, LoginFailMonitor>) pattern -> {
            LoginEvent begin = pattern.getOrDefault("begin", null).iterator().next();
            LoginEvent next = pattern.getOrDefault("next", null).iterator().next();
            return new LoginFailMonitor(next.getUserId(), begin.getIp(), next.getIp(), next.getResult());
        }).print();

        env.execute("Login Fail Monitor");
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

    @Data
    @AllArgsConstructor
    static class LoginFailMonitor {
        Integer userId;
        String startIp;
        String nextIp;
        String result;
    }
}
