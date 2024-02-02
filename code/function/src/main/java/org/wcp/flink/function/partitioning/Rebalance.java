package org.wcp.flink.function.partitioning;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wcp.flink.sourcefunction.pojotype.Person;

import java.util.ArrayList;
import java.util.List;

public class Rebalance {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Person> people = new ArrayList<>();

        people.add(new Person("Fred", 37));
        people.add(new Person("Wilma", 35));
        people.add(new Person("Pebbles", 4));
        people.add(new Person("yyl", 18));

        DataStream<Person> flintstones = env.fromCollection(people);

        // Flink默认分区策略就是 rebalance
        flintstones
                .rebalance()
                .print("===>>>")
                .setParallelism(4);

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
