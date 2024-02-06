package org.wcp.flink.source.addsource.custom.parallelsourcefunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;
public class ParallelIntegerSequence {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Long> flintstones = env.addSource(new ParallelSourceFunction<Long>() {
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