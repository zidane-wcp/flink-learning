package org.wcp.flink.source.fromsource.custom.integersequence;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Long> flintstones = env.fromSource(new NumberSequenceSource(0, 100), WatermarkStrategy.noWatermarks(), "test");

        flintstones.print("===>>>");

        env.execute("FromSequence Demo");
    }
}
