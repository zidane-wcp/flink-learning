package org.wcp.flink.source.addsource.custom.richsourcefunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class MySQLReaderJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new MySQLReader())
                // Print类型的Sink都是调用元素的toString()方法转换成字符串并打印，所以User类中要实现toString()方法
                .addSink(new PrintSinkFunction<>());

        env.execute("Source Function test");
    }
}
