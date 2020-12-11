package com.xzc.state;

import com.xzc.apitest.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

public class Demo1 {
    static final SimpleDateFormat YYYY_MM_DD_HH = new SimpleDateFormat("yyyyMMdd HH");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        List<Order> data = new LinkedList<>();
        for (long i = 1; i <= 25; i++)
            data.add(new Order(i, i % 7, i % 3, new BigDecimal((i + 0.1) + "")));

        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = env.fromCollection(data).assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
            @Override
            public long extractTimestamp(Order element, long recordTimestamp) {
                return element.finishTime;
            }
        }));
        orderSingleOutputStreamOperator.print("原始数据");

//        orderSingleOutputStreamOperator.keyBy((KeySelector<Order, Object>) vale -> vale.memberId).map(new RichMapFunction<Order, String>() {
//            MapState<Long, Order> mapState;
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//
//                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Order>("mapState", Long.TYPE, Order.class));
//            }
//            @Override
//            public String map(Order value) throws Exception {
//                if (mapState.contains(value.productId)) {
//                    Order acc = mapState.get(value.productId);
//                    value.sale = value.sale.add(acc.sale);
//                }
//                mapState.put(value.productId, value);
//                // 当前店铺 商品 销售额统计
//                StringBuilder stringBuilder = new StringBuilder("");
//                stringBuilder.append("当前店铺ID：").append(value.memberId).append(" \n ");
//                stringBuilder.append("==============-=========").append(" \n ");
//                List<Order> list = IteratorUtils.toList(mapState.values().iterator());
//                for (Order order: list) {
//                    stringBuilder.append("商品Id： ").append(order.productId);
//                    stringBuilder.append(" ；累计销售额：").append(order.sale).append(" \n ");
//                }
//                stringBuilder.append("==============-=========");
//                return stringBuilder.toString();
//            }
//        }).print();

        // 优化 处理延迟数据
        orderSingleOutputStreamOperator.keyBy((KeySelector<Order, Object>) vale -> vale.memberId).map(new RichMapFunction<Order, Object>() {
            // key为事件时间的"yyyyMMdd HH" 字符串
            MapState<String, MemberRank> mapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                StateTtlConfig build = StateTtlConfig.newBuilder(Time.hours(1))
                        //设置ttl更新策略为创建和写，直观作用为如果一个key（例如20200101 01）1个小时内没有写入的操作，只有读的操作，那么这个key将被标记为超时
                        //值得注意的是，MapState ListState这类集合state，超时机制作用在每个元素上，也就是每个元素的超时是独立的
                        .updateTtlOnCreateAndWrite().cleanupFullSnapshot().build();
                MapStateDescriptor<String, MemberRank> hourRank = new MapStateDescriptor<>("hourRank", String.class, MemberRank.class);
                hourRank.enableTimeToLive(build);

                mapState = getRuntimeContext().getMapState(hourRank);
            }

            @Override
            public MemberRank map(Order value) throws Exception {
                String key = YYYY_MM_DD_HH.format(value.finishTime);
                MemberRank rank;
                if(mapState.contains(key)){
                    rank = mapState.get(key);
                    rank.merge(value);
                }else{
                    rank = MemberRank.of(value);
                }
                mapState.put(key,rank);
                return rank;
            }
        }).print();

        env.execute("1111");

    }

    public static class Order {
        //finishTime: Long, memberId: Long, productId: Long, sale: Double
        public long finishTime;
        public long memberId;
        public long productId;
        public BigDecimal sale;

        public Order() {
        }

        public Order(Long finishTime, Long memberId, Long productId, BigDecimal sale) {
            this.finishTime = finishTime;
            this.memberId = memberId;
            this.productId = productId;
            this.sale = sale;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "finishTime=" + finishTime +
                    ", memberId=" + memberId +
                    ", productId=" + productId +
                    ", sale=" + sale +
                    '}';
        }
    }

    public static class MemberRank {
        public String time;
        public long memberId;
        public Vector<Order> rank;

        public MemberRank() {
        }

        public MemberRank(String time, long memberId, Vector<Order> rank) {
            this.time = time;
            this.memberId = memberId;
            this.rank = rank;
        }

        public static MemberRank of(Order o) {
            // 只存一个商品
            Vector<Order> orders = new Vector<>();
            orders.add(o);
            return new MemberRank(YYYY_MM_DD_HH.format(o.finishTime), o.memberId, orders);
        }

        public void merge(Order o) {
            Boolean flag = true;
            for (Order list: rank) {
                if (list.productId == o.productId) {
                    list.sale = list.sale.add(o.sale);
                    flag = false;
                }
            }
            if (flag) {
                rank.add(o);
            }
            rank.sort((o1, o2) -> (o1.sale.subtract(o2.sale)).multiply(new BigDecimal(1000)).intValue());
        }

        @Override
        public String toString() {
            return "MemberRank{" +
                    "time='" + time + '\'' +
                    ", memberId=" + memberId +
                    ", rank=" + rank +
                    '}';
        }
    }
}
