package util;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class FlinkUtils {

    static StreamExecutionEnvironment env;
    static EnvironmentSettings bsSettings;
    static StreamTableEnvironment tEnv;
    static Configuration conf;
    static StatementSet statementSet;



    static {
        //flink执行环境
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ckp频率
        env.enableCheckpointing(30 * 1000L);
        //ckp精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //ckp间隔时长
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000L);
        //ckp超时限制
        env.getCheckpointConfig().setCheckpointTimeout(15 * 60 * 1000L);
        //ckp容错次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        //ckp并存数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //ckp容灾机制
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                100, // 尝试重启的次数
                Time.of(60, TimeUnit.SECONDS) // 延时
        ));
        //开启批模式
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, bsSettings);
        conf = tEnv.getConfig().getConfiguration();
        //minibatch设置
        conf.setString("table.exec.mini-batch.enabled", "true");
        conf.setString("table.exec.mini-batch.allow-latency", "2s");
        conf.setString("table.exec.mini-batch.size", "5000");
        //kafka上有去重算子
        conf.setString("table.exec.source.cdc-events-duplicate", "true");

        statementSet = tEnv.createStatementSet();
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }

    public static StreamTableEnvironment gettEnv() {
        return tEnv;
    }

    public static StatementSet getStatementSet() {
        return statementSet;
    }
}
