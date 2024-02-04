package org.wcp.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

public class IteratorSinkDemo {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Long> nums = env.fromSequence(1, 10);
        Iterator<Long> myOutput = DataStreamUtils.collect(nums);

        while (myOutput.hasNext()){
            System.out.println("===>>>" + myOutput.next());
        }

        env.execute("IteratorSinkDemo");
    }
}
