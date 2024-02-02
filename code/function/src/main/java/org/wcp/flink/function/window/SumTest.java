package org.wcp.flink.function.window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SumTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStream<Tuple3<String, String, Integer>> flintstones = env.fromElements(
                new Tuple3<>("Fred", "11", 37),
                new Tuple3<>("Wilma", "22", 35),
                new Tuple3<>("Pebbles", "33", 2),
                new Tuple3<>("Pebbles", "44", 1)
        );

        flintstones
                .keyBy(tuple -> tuple.f0)
                .sum(2)
                .print("===>>>");

        env.execute("Flink Java API Skeleton");
    }
}
