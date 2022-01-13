package test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import util.FlinkUtils;

public class a {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        StatementSet statementSet = FlinkUtils.getStatementSet();
        StreamTableEnvironment tEnv = FlinkUtils.gettEnv();


//        MySqlSource<Object> mySqlSource = MySqlSource.builder()
//                .hostname("")
//                .port(3306)
//                .databaseList("")
//                .tableList("")
//                .username("")
//                .password("")
////                .deserializer(new StringDebeziumDeserializationSchema())
//                .build();

        tEnv.executeSql("create table `product_brand` (\n" +
                "   id BIGINT,\n" +
                "   zid BIGINT,\n" +
                "   title STRING,\n" +
                "   sort BIGINT,\n" +
                "   gmt_modified TIMESTAMP,\n" +
                "   gmt_create TIMESTAMP," +
                "   primary key (`id`) NOT ENFORCED\n" +
                ")with(\n" +
                "            'connector' = 'mysql-cdc',\n" +
                "            'hostname' = '8.134.35.221',\n" +
                "            'port' = '3306',\n" +
                "            'username' = 'root',\n" +
                "            'password' = 'root',\n" +
                "            'database-name' = 'test',\n" +
                "            'table-name' = 'product_brand',\n" +
                "            'debezium.snapshot.mode' = 'initial',\n" +
                "            'debezium.snapshot.locking.mode' = 'none')");

        Table table = tEnv.sqlQuery("select * from product_brand");
        tEnv.toRetractStream(table, Row.class).print();


        env.execute("test a");
    }
}
