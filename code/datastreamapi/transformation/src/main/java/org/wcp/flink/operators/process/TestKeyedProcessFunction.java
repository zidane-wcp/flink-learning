/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wcp.flink.operators.process;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.wcp.flink.sourcefunction.customsource.TaxiFareSourceFunction;
import org.wcp.flink.sourcefunction.customsource.pojotype.TaxiFare;

import static org.wcp.flink.operators.process.TestKeyedProcessFunction.PseudoWindow.lateFares;

public class TestKeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStream<TaxiFare> fares = env
                .addSource(new TaxiFareSourceFunction())
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order
                        WatermarkStrategy.<TaxiFare>forMonotonousTimestamps().withTimestampAssigner((fare, t) -> fare.getEventTimeMillis()));

        // compute tips per hour for each driver
        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyTips =
                fares
                        .keyBy((TaxiFare fare) -> fare.driverId)
                        .process(new PseudoWindow(Time.minutes(1)));

        hourlyTips.addSink(new PrintSinkFunction<>());
        hourlyTips.getSideOutput(lateFares).print();

//        // find the driver with the highest sum of tips for each hour
//        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
//                hourlyTips
//                        .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
//                        .maxBy(2);
//
//        /* You should explore how this alternative (commented out below) behaves.
//         * In what ways is the same as, and different from, the solution above (using a windowAll)?
//         */
//
//        // DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips.keyBy(t -> t.f0).maxBy(2);
//        hourlyMax.addSink(new PrintSinkFunction<>());


        env.execute("test job");
    }


    // Compute the sum of the tips for each driver in hour-long windows.
    // The keys are driverIds.
    public static class PseudoWindow extends KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {
        private final long durationMsec;

        // Keyed, managed state, with an entry for each window, keyed by the window's end time.
        // There is a separate MapState object for each driver.
        private transient MapState<Long, Float> sumOfTips;
        protected static final OutputTag<TaxiFare> lateFares = new OutputTag<TaxiFare>("lateFares") {};
        public PseudoWindow(Time duration) {
            this.durationMsec = duration.toMilliseconds();
        }

        @Override
        // Called once during initialization.
        public void open(Configuration conf) {
            MapStateDescriptor<Long, Float> sumDesc =
                    new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
            sumOfTips = getRuntimeContext().getMapState(sumDesc);
        }

        @Override
        // Called as each fare arrives to be processed.
        public void processElement(
                TaxiFare fare,
                Context ctx,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {


            long eventTime = fare.getEventTimeMillis();
            TimerService timerService = ctx.timerService();

            if (eventTime <= timerService.currentWatermark()) {
                // This event is late; its window has already been triggered.
                ctx.output(lateFares, fare);
            } else {
                // Round up eventTime to the end of the window containing this event.
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                // Schedule a callback for when the window has been completed.
                timerService.registerEventTimeTimer(endOfWindow);

                // Add this fare's tip to the running total for that window.
                Float sum = sumOfTips.get(endOfWindow);
                if (sum == null) {
                    sum = 0.0F;
                }
                sum += fare.tip;
                this.sumOfTips.put(endOfWindow, sum);
            }
        }

        @Override
        // Called when the current watermark indicates that a window is now complete.
        public void onTimer(long timestamp,
                            OnTimerContext context,
                            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

            long driverId = context.getCurrentKey();
            // Look up the result for the hour that just ended.
            Float sumOfTips = this.sumOfTips.get(timestamp);

            Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
            out.collect(result);
            this.sumOfTips.remove(timestamp);
        }
    }
}
