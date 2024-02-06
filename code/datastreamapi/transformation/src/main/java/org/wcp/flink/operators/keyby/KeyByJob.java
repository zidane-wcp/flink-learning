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

package org.wcp.flink.operators.keyby;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wcp.flink.source.addsource.custom.sourcefunction.TaxiRideSourceFunction;
import org.wcp.flink.source.pojotype.EnrichedRide;
import org.wcp.flink.source.pojotype.TaxiRide;

public class KeyByJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<TaxiRide> rides = env.addSource(new TaxiRideSourceFunction());
        SingleOutputStreamOperator<EnrichedRide> enrichedRides = rides.map(new EnrichmentMapFunction());
        DataStream<Tuple2<Short, Integer>> passengerCntByStartCell = enrichedRides.flatMap(new TupleFlatMapFunction());

        passengerCntByStartCell
                .keyBy(value -> value.f0)
                .maxBy(1)
                .print();

        env.execute("Taxi Ride Cleansing");
    }
    public static class EnrichmentMapFunction implements MapFunction<TaxiRide, EnrichedRide> {

        @Override
        public EnrichedRide map(TaxiRide taxiRide) {
            return new EnrichedRide(taxiRide);
        }
    }

    public static class TupleFlatMapFunction implements FlatMapFunction<EnrichedRide, Tuple2<Short, Integer>> {

        @Override
        public void flatMap(EnrichedRide enrichedRide, Collector<Tuple2<Short, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(enrichedRide.passengerCnt, enrichedRide.startCell));
        }
    }
}
