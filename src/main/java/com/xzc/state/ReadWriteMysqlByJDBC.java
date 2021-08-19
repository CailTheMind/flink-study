package com.xzc.state;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class ReadWriteMysqlByJDBC {

    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
//        //需要与SQL中获取的字段类型一一对应，否则会取不到值
//        TypeInformation[] fieldTypes = new TypeInformation[] {
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO};
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
//
//        //读mysql
//        DataSet<Row> dataSource = fbEnv.createInput(JDBCInputFormat.buildJDBCInputFormat()
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setDBUrl("jdbc:mysql://XXX.XX.XX.XX:3306/bdmdb?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true")
//                .setUsername("XXXX")
//                .setPassword("XXXX")
//                .setQuery("select name,age from staff")
//                .setRowTypeInfo(rowTypeInfo)
//                .finish());
//
//
//        System.out.println("总共查询出来的数据行："+dataSource.count());
//        dataSource.print();//此处打印
//
//        //写MySQL
//        dataSource.output(JDBCOutputFormat.buildJDBCOutputFormat()
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setDBUrl("jdbc:mysql://XXX.XX.XX.XX:3307/bdmdb?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true")
//                .setUsername("XXXX")
//                .setPassword("XXXXX")
////                    .setQuery("update admin_user set nick_name = 'QWQZXG' where username = 'eeeeWQQQ'") //这句sql需要调整
//                .setQuery("insert into staff values(?,?)")
//
//                .finish());
//
//        fbEnv.execute();
    }
}
