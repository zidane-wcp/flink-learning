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

package org.wcp.flink.operators.map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.wcp.flink.source.customsource.TaxiRideSourceFunction;
import org.wcp.flink.source.customsource.pojotype.EnrichedRide;
import org.wcp.flink.source.customsource.pojotype.TaxiRide;
import org.wcp.flink.source.customsource.utils.GeoUtils;

public class RideMapFunctionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new TaxiRideSourceFunction())
                .filter(new NYCFilterFunction())
                .map(new EnrichmentMapFunction())
                .addSink(new PrintSinkFunction<>());

        env.execute("Taxi Ride Cleansing");
    }
    public static class NYCFilterFunction implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                    && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
    public static class EnrichmentMapFunction implements MapFunction<TaxiRide, EnrichedRide> {

        @Override
        public EnrichedRide map(TaxiRide taxiRide) {
            return new EnrichedRide(taxiRide);
        }
    }
}
