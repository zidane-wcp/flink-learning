package org.wcp.flink.operators.partitioning;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wcp.flink.source.pojotype.Person;

import java.util.ArrayList;
import java.util.List;

public class Shuffle {
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

        flintstones
                .shuffle()
                .print("===>>>")
                .setParallelism(4);

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }

//    public static class Person {
//        public String name;
//        public Integer age;
//        public Person() {}
//
//        public Person(String name, Integer age) {
//            this.name = name;
//            this.age = age;
//        }
//        public String toString() {
//            return this.name + ": age " + this.age.toString();
//        }
//    }
}
