package com.xzc.demo1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理 分词统计
 *
 * @author xzc
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 针对当前执行环境设置并行度（线程数） 默认服务器核心数
        env.setParallelism(2);
        // 接收socket文本流
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7878);

        // string 为数据，Tuple2 为处理的结果，将数据处理成Tuple2这样的
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] splitArray = value.split("\\s");
                for (String str : splitArray) {
                    Tuple2<String, Long> tuple2 = new Tuple2<String, Long>();
                    tuple2.f1 = 1L;
                    tuple2.f0 = str;
                    out.collect(tuple2);
                }
            }
        }).filter(new FilterFunction<Tuple2<String, Long>>() {
            public boolean filter(Tuple2<String, Long> value) throws Exception {
                return value != null;
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, Object>() { // 数据分区，通过key的hashcode去分到线程中分开不同执行sum
            public Object getKey(Tuple2<String, Long> value) {
                return value.f0;
            }
        }).sum(1).print();

        env.execute("流处理 单词统计");
    }
}
