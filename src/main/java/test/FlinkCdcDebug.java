package test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FlinkUtils;

import java.util.ArrayList;

public class FlinkCdcDebug {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCdcDebug.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        ArrayList<RowType.RowField> arrayList = new ArrayList<>();
        RowType.RowField id = new RowType.RowField("id", new IntType(), "id");
        RowType.RowField envKey = new RowType.RowField("env_key", new CharType(true,50), "env_key");
        RowType.RowField companyId = new RowType.RowField("company_id", new CharType(true,100), "company_id");
        arrayList.add(id);
        arrayList.add(envKey);
        arrayList.add(companyId);
        RowType rowType = new RowType(true, arrayList);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.49.0.86")
                .port(3306)
                .username("bdp")
                .password("akd_bdp")
                .databaseList("dev_erp")
                .tableList("dev_erp.student")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql source");
        dataStreamSource.print();
        env.execute();
    }
}
