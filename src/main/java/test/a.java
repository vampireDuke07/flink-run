package test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkUtils;

public class a {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        StatementSet statementSet = FlinkUtils.getStatementSet();
        StreamTableEnvironment tEnv = FlinkUtils.gettEnv();


        MySqlSource<Object> mySqlSource = MySqlSource.builder()
                .hostname("")
                .port(3306)
                .databaseList("")
                .tableList("")
                .username("")
                .password("")
//                .deserializer(new StringDebeziumDeserializationSchema())
                .build();



        env.execute();
    }
}
