package org.wcp.flink.function.iterate;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStream<Long> someIntegers = env.fromSequence(0, 10);
        // 创建迭代数据流
        IterativeStream<Long> iteration = someIntegers.iterate();
        // 对于迭代数据流，需要做一些逻辑，使其最终能够收敛，否则就是无限迭代
        // 对于迭代数据流，经过以下map算子之后，还会进入iteration这个迭代流中，继续进入map算子。
        DataStream<Long> iterationBody = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });
        // 以上的在map算子上应用迭代流，就相当于做while循环。
        // 以下对iterationBody应用filter算子，就相当于while()循环中的条件，即当value>0时才会循环，也就是 while(value>0)。
        DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });
        // 设置完循环条件后，应用到迭代流上。
        iteration.closeWith(feedback);
        iterationBody.print();
        env.execute();
    }
}
