package com.xzc.flinksql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class Demo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(fsEnv,fsSettings);

        DataStream<Order> orderA = fsEnv.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = fsEnv.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        Table table2 = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        tEnv.createTemporaryView("orderA", table2);

        String execSQL = ""
                + "SELECT userID, eventType, eventTime, productID  "
                + "FROM ( "
                + "  SELECT *, "
                + "     ROW_NUMBER() OVER (PARTITION BY user, product, amount ORDER BY amount DESC) AS rownum "
                + "  FROM orderA "
                + ") t "
                + "WHERE rownum = 1";

        tEnv.sqlQuery(execSQL).execute().print();
        fsEnv.execute("1111");
//        streamTableEnvironment.sqlQuery("SELECT * FROM OrderA").execute().print();
        // register DataStream as Table
//        tEnv.createTemporaryView("OrderA", orderA, $("user"), $("product"), $("amount"));
//        tEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));
////        tEnv.sqlQuery("SELECT * FROM OrderA").execute().print();
//        tEnv.from("OrderB").select($("product"), $("amount")).where($("amount").isNotNull()).execute().print();
//        env.execute("111");
//        tEnv.execute("222");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public Integer amount;
    }

}


