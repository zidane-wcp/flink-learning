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

package org.wcp.flink.operators.flatmap;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.wcp.flink.operators.map.RideMapFunctionJob;
import org.wcp.flink.source.addsource.custom.sourcefunction.TaxiRideSourceFunction;
import org.wcp.flink.source.pojotype.EnrichedRide;
import org.wcp.flink.source.pojotype.TaxiRide;

public class RideFlatMapFunctionJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new TaxiRideSourceFunction())
                .flatMap(new NYCEnrichmentFlatMapFunction())
                .addSink(new PrintSinkFunction<>());

        env.execute("Taxi Ride Cleansing");
    }

    // FlatMapFunction中可以多次调用out.collect()方法，所以对于一条element，可以emit出来0条或多条。
    public static class NYCEnrichmentFlatMapFunction implements FlatMapFunction<TaxiRide, EnrichedRide> {

        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> valid = new RideMapFunctionJob.NYCFilterFunction();
            // 过滤掉不在纽约市的TaxiRide
            if (valid.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }
}