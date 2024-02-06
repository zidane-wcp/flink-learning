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

package org.wcp.flink.source.addsource.custom.sourcefunction;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.wcp.flink.source.pojotype.TaxiFare;
import org.wcp.flink.source.utils.DataGenerator;

import java.time.Duration;
import java.time.Instant;

/**
 * This SourceFunction generates a data stream of TaxiFare records.
 *
 * <p>The stream is generated in order.
 */
public class TaxiFareSourceFunction implements SourceFunction<TaxiFare> {
    private volatile boolean running = true;
    private Instant limitingTimestamp = Instant.MAX;

    /** Create a bounded TaxiFareGenerator that runs only for the specified duration. */
    public static TaxiFareSourceFunction runFor(Duration duration) {
        TaxiFareSourceFunction generator = new TaxiFareSourceFunction();
        generator.limitingTimestamp = DataGenerator.BEGINNING.plus(duration);
        return generator;
    }
    @Override
    public void run(SourceContext<TaxiFare> ctx) throws Exception {

        long id = 1;
        while (running) {
            TaxiFare fare = new TaxiFare(id);

            // don't emit events that exceed the specified limit
            if (fare.startTime.compareTo(limitingTimestamp) >= 0) {
                break;
            }

            ++id;
            ctx.collect(fare);

            // match our event production rate to that of the TaxiRideGenerator
            Thread.sleep(1000L);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}
