package org.wcp.flink.source.addsource.custom.sourcefunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class IntegerSequence {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Long> flintstones = env.addSource(new SourceFunction<Long>() {
            private volatile boolean running = true;
            final Random random = new Random();
            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                while (running){
                    ctx.collect(random.nextLong());
                    Thread.sleep(1000L);
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        });

        flintstones.print("===>>>");
        env.execute("FromSequence Demo");
    }
}