package app;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.relational.TableSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkUtils;

import javax.swing.table.TableColumn;

public class Flink2SR {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        StatementSet statementSet = FlinkUtils.getStatementSet();
        StreamTableEnvironment tEnv = FlinkUtils.gettEnv();


//        RowDataDebeziumDeserializeSchema deserializeSchema = new RowDataDebeziumDeserializeSchema(
//
//        );

        MySqlSource.builder()
                .hostname("10.49.0.86")
                .port(3306)
                .databaseList("dev_erp")
                .tableList("dev_erp.product_brand_bak")
//                .deserializer()
        .build();

    }
}
