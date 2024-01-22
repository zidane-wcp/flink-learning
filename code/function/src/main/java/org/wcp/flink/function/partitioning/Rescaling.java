package org.wcp.flink.function.partitioning;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Rescaling {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new RichParallelSourceFunction<Object>() {
            @Override
            public void run(SourceContext<Object> ctx) throws Exception {
                for (int i = 0; i < 20; i++){
                    // 将偶数发送到索引为0的子任务，将奇数发送到索引为1的子任务。
                    if ((i + 1) % 2 ==getRuntimeContext().getIndexOfThisSubtask()){
                        ctx.collect(i + 1);
                    }
                }
            }
            @Override
            public void cancel() {
            }
        })
                .setParallelism(2)
                .rescale()
                .print("===>>>")
                .setParallelism(4);
                // 输出结果中，索引为1、2的子任务打印的全部是偶数，索引为3、4的子任务打印的全部是奇数。

        env.execute();
    }
}
