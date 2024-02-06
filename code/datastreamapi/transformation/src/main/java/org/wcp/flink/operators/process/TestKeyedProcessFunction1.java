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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.wcp.flink.source.addsource.custom.sourcefunction.TaxiRideSourceFunction;
import org.wcp.flink.source.pojotype.TaxiRide;

import java.time.Duration;

/**
 * Java solution for the "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class TestKeyedProcessFunction1 {

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSourceFunction());

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(new PrintSinkFunction<>());

        env.execute("Long Taxi Rides");
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {
        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> rideStateDescriptor =
                    new ValueStateDescriptor<>("ride event", TaxiRide.class);
            rideState = getRuntimeContext().getState(rideStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {

            TaxiRide firstRideEvent = rideState.value();

            if (firstRideEvent == null) {
                // whatever event comes first, remember it
                rideState.update(ride);

                if (ride.isStart) {
                    // we will use this timer to check for rides that have gone on too long and may
                    // not yet have an END event (or the END event could be missing)
                    context.timerService().registerEventTimeTimer(getTimerTime(ride));
                }
            } else {
                if (ride.isStart) {
                    if (rideTooLong(ride, firstRideEvent)) {
                        out.collect(ride.rideId);
                    }
                } else {
                    // the first ride was a START event, so there is a timer unless it has fired
                    context.timerService().deleteEventTimeTimer(getTimerTime(firstRideEvent));

                    // perhaps the ride has gone on too long, but the timer didn't fire yet
                    if (rideTooLong(firstRideEvent, ride)) {
                        out.collect(ride.rideId);
                    }
                }

                // both events have now been seen, we can clear the state
                // this solution can leak state if an event is missing
                // see DISCUSSION.md for more information
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {

            // the timer only fires if the ride was too long
            out.collect(rideState.value().rideId);

            // clearing now prevents duplicate alerts, but will leak state if the END arrives
            rideState.clear();
        }

        private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            // 大于30分钟，就视为long ride
            return Duration.between(startEvent.eventTime, endEvent.eventTime).compareTo(Duration.ofMinutes(30)) > 0;
        }

        private long getTimerTime(TaxiRide ride) throws RuntimeException {
            if (ride.isStart) {
                return ride.eventTime.plusSeconds(120 * 60).toEpochMilli();
            } else {
                throw new RuntimeException("Can not get start time from END event.");
            }
        }
    }
}
