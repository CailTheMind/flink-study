package com.xzc.state;

import com.xzc.apitest.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Demo2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        List<SensorReading> sensorReadingList = new ArrayList<SensorReading>();
        sensorReadingList.add(new SensorReading("sensor_1", 1547718199L, 35.2));
        sensorReadingList.add(new SensorReading("sensor_2", 1547718598L, 33.1));
        sensorReadingList.add(new SensorReading("sensor_3", 1547718456L, 32.4));
        sensorReadingList.add(new SensorReading("sensor_4", 1547718685L, 31.5));
        sensorReadingList.add(new SensorReading("sensor_5", 1547718325L, 30.6));
        sensorReadingList.add(new SensorReading("sensor_6", 1547718128L, 21.7));

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.fromCollection(sensorReadingList);
        sensorReadingDataStreamSource.keyBy(new KeySelector<SensorReading, Object>() {
            @Override
            public Object getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });
    }
    static class MyRichFlatMapFunction extends RichFlatMapFunction<SensorReading, String> {
        public MyRichFlatMapFunction(Double threshold) {
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {

        }
    }

    static class MyRichMapFunction extends RichMapFunction<SensorReading, String> {
        // 全局状态
        ValueState<Double> valueState;
        ListState<Integer> listState;
        MapState<String, Double> mapState;
        ReducingState<SensorReading> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态 变量名不能一样
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("valueState", Double.TYPE));

            listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.TYPE));

            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("mapState", String.class, Double.TYPE));

            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("reducingState", new ReduceFunction<SensorReading>() {
                @Override
                public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                    return null;
                }
            }, SensorReading.class));

            super.open(parameters);
        }

        @Override
        public String map(SensorReading value) throws Exception {
            // 读取状态
            valueState.value();
            // 更新状态
            valueState.update(value.getTemperature());

            // 获取状态
            Iterable<Integer> doubles = listState.get();
            // 添加状态
            listState.add(1);

//            listState.update();

            mapState.keys();
            mapState.get("111");

            SensorReading sensorReading = reducingState.get();
            reducingState.add(sensorReading);
            return null;
        }
    }
}
