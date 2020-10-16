package com.xzc.demo1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 单词统计
 *
 * @author xzc
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String filePath = "E:\\IdeaProjects\\flink-study\\src\\main\\resources\\hello.txt";

        DataSource<String> fileDataSource = env.readTextFile(filePath);


        fileDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            // 处理数据 打散拆分
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] splitArray = value.split("\\s");
                for (String str : splitArray) {
                    Tuple2<String, Long> tuple2 = new Tuple2<String, Long>();
                    tuple2.f1 = 1L;
                    tuple2.f0 = str;
                    out.collect(tuple2);
                }
            }
        })
                .groupBy(0) // 以第一个元素做分组
                .sum(1) // 以第二个元素做sum求和
                .print();

    }
}
