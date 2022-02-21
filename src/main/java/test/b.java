package test;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.StarRocksSinkRowDataWithMeta;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import deserialization.StarRocksDebeziumDeserializationSchema;
import entity.ParaManager;
import function.BasicMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class b {
    public static void main(String[] args) throws Exception {
        //环境相关配置
        ParameterTool params = ParameterTool.fromArgs(args);
        ParaManager paraManager = new ParaManager(params);

        Configuration configuration = new Configuration();
        configuration.setString("pipeline.name", paraManager.getJobName());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(paraManager.getCheckpointInterval());

        env.setParallelism(paraManager.getShuffleParallelism());

        Properties props = new Properties();
        props.setProperty("event.deserialization.failure.handling.mode", "warn");
        props.setProperty("min.row.count.to.stream.results", "0");
        props.setProperty("scan.incremental.snapshot.chunk.size", "80960");

        //mysql数据源
        MySqlSourceBuilder<String> stringMySqlSourceBuilder = MySqlSource.<String>builder()
                .hostname(paraManager.getSourceHost().trim())
                .port(paraManager.getSourcePort())
                .databaseList(paraManager.getSourceDatabase().trim())
                .username(paraManager.getSourceUserName().trim())
                .password(paraManager.getSourcePassWord())
                .debeziumProperties(props);

        stringMySqlSourceBuilder.serverId(paraManager.getServerId());

        if (paraManager.getSourceTables() == null) {
            stringMySqlSourceBuilder.tableList(".*");
        } else {
            stringMySqlSourceBuilder.tableList(paraManager.getSourceTables());
        }

        MySqlSource<String> mySqlSource = stringMySqlSourceBuilder
                .deserializer(new StarRocksDebeziumDeserializationSchema(paraManager.getEnvKey(), paraManager.getSinkDatabase()))
                .build();

        //输出SR
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "flink_sr_test")
                .uid("uid_mysql_source")
                .setParallelism(paraManager.getSourceParallelism())
                .map(new BasicMapFunction()).uid("uid_map")
                .keyBy((KeySelector<StarRocksSinkRowDataWithMeta, String>) value ->
                        String.format("%s.%s.%s", value.getDatabase(), value.getTable(), value.getPrimaryKey()))
                .addSink(
                        StarRocksSink.sinkWithMeta(
                                StarRocksSinkOptions.builder()
                                        .withProperty("jdbc-url", String.format("jdbc:mysql://%s", paraManager.getSinkJDBCHost()))
                                        .withProperty("load-url", paraManager.getSinkHost())
                                        .withProperty("database-name", ".*")
                                        .withProperty("table-name", ".*")
                                        .withProperty("username", paraManager.getSinkUserName())
                                        .withProperty("password", paraManager.getSinkPassword())
                                        .withProperty("sink.max-retries", "4")
                                        .withProperty("sink.buffer-flush.max-bytes", paraManager.getSinkMaxBatchSize())
                                        .withProperty("sink.buffer-flush.interval-ms", paraManager.getSinkInterval())
                                        .withProperty("sink.buffer-flush.max-rows", paraManager.getSinkMaxRow())
                                        .withProperty("sink.properties.format", "json")
                                        .withProperty("sink.properties.strip_outer_array", "true")
                                        .withProperty("sink.properties.max_filter_ratio", paraManager.getMaxFilterRate())
                                        .build()))
                .uid("uid_sink")
                .name("star_rocks")
                .setParallelism(paraManager.getSinkParallelism());

        env.execute();

    }
}
