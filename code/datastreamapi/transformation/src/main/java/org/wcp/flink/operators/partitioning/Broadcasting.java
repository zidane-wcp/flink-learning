package org.wcp.flink.operators.partitioning;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wcp.flink.sourcefunction.pojotype.Person;

import java.util.ArrayList;
import java.util.List;

public class Broadcasting {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Person> people = new ArrayList<>();
        people.add(new Person("yyl", 18));
        DataStream<Person> flintstones = env.fromCollection(people).setParallelism(1);

        flintstones
                .broadcast()
                .print("===>>>")
                .setParallelism(2);

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
