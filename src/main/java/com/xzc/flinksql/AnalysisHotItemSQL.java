package com.xzc.flinksql;

import com.xzc.apitest.StationLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormatter;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormatterBuilder;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author xzc
 */
public class AnalysisHotItemSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String path = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\UserBehavior.txt";
        DataStream<String> inputStream = env.readTextFile(path);

        DataStream<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] dataArray = StringUtils.split(s,",");
                return new UserBehavior(Long.parseLong(dataArray[0]), Long.parseLong(dataArray[1]), Integer.parseInt(dataArray[2]), dataArray[3], Long.parseLong(dataArray[4]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        }));

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.createTemporaryView("UserBehavior", dataStream, $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));
        String sql = "select * from (" +
                "select *, row_number() over(partition by windowEnd order by cnt desc) as row_num from(" +
                "select itemId, count(itemId) as cnt, hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd from " +
                "UserBehavior where behavior = 'pv' group by itemId, hop(ts, interval '5' minute, interval '1' hour))) where row_num <= 5";

        Table topNResultTable = tableEnv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(topNResultTable, Row.class);
        tuple2DataStream.print();

        SingleOutputStreamOperator<UserBehavior> pv = dataStream.filter((FilterFunction<UserBehavior>) value -> value.getBehavior().equals("pv"));
        KeyedStream<UserBehavior, Object> userBehaviorObjectKeyedStream = pv.keyBy((KeySelector<UserBehavior, Object>) value -> value.getItemId());
        userBehaviorObjectKeyedStream.print("11111");
        SingleOutputStreamOperator<ItemViewCount> aggregate = userBehaviorObjectKeyedStream.timeWindow(Time.minutes(1), Time.hours(1))
                .aggregate(new CountAgg(), new WindowFunction<Long, ItemViewCount, Object, TimeWindow>() {
                    @Override
                    public void apply(Object o, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
                        out.collect(new ItemViewCount(Long.valueOf(o.toString()), window.getEnd(), input.iterator().next()));
                    }
                });
        aggregate.print("22222");
        KeyedStream<ItemViewCount, Object> itemViewCountObjectKeyedStream = aggregate.keyBy(new KeySelector<ItemViewCount, Object>() {
            @Override
            public Object getKey(ItemViewCount value) throws Exception {
                return value.getWindowEnd();
            }
        });
        itemViewCountObjectKeyedStream.print("333333");
        itemViewCountObjectKeyedStream.process(new TopNList(3)).addSink(new RichSinkFunction<List<ItemViewCount>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }
            @Override
            public void close() throws Exception {
                super.close();
            }
            @Override
            public void invoke(List<ItemViewCount> value, Context context) throws Exception {
                System.out.println(value);
            }
        });
        env.execute("Top PV");
    }

    public static class TopN extends KeyedProcessFunction<Object, ItemViewCount, String> {

        private final int n;

        public TopN(int n) {
            this.n = n;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private transient ListState<ItemViewCount> itemState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每条数据都保存到状态中
            this.itemState.add(value);
            // 注册 windowEnd + 1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });

            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i = 0; i < n; i++) {
                ItemViewCount currentItem = allItems.get(i);
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.getItemId())
                        .append("  浏览量=").append(currentItem.getCount())
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }

    public static class TopNList extends KeyedProcessFunction<Object, ItemViewCount, List<ItemViewCount>> {

        private final int n;

        public TopNList(int n) {
            this.n = n;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private transient ListState<ItemViewCount> itemState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<List<ItemViewCount>> out) throws Exception {
            // 每条数据都保存到状态中
            this.itemState.add(value);
            // 注册 windowEnd + 1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<ItemViewCount>> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });

            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            List<ItemViewCount> list  = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                list.add(allItems.get(i));
            }
            result.append("====================================\n\n");

            out.collect(list);
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class WindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        /**
         * 窗口的主键，即 itemId
         * 窗口
         * 聚合函数的结果，即 count 值
         * 输出类型为 ItemViewCount
         */
        @Override
        public void apply(Long aLong, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(new ItemViewCount(aLong, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }
}


