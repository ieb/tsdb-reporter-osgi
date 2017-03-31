/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sling.tsdb;

import com.codahale.metrics.*;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by boston on 29/03/2017.
 */
public class TSDBFileReporter extends ScheduledReporter {

    public static Builder forRegistry(MetricRegistry metricRegistry) {

        return new Builder(metricRegistry);
    }


    public static class Builder {

        private MetricRegistry metricRegistry;
        private TimeUnit ratesUnit;
        private TimeUnit durationUnit;

        public Builder(MetricRegistry metricRegistry) {

            this.metricRegistry = metricRegistry;
        }

        public Builder convertRatesTo(TimeUnit ratesUnit) {
            this.ratesUnit = ratesUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder tag(String key, String value) {
            return this;
        }

        public ScheduledReporter build() {
            return new TSDBFileReporter(metricRegistry, "RRDReporter", MetricFilter.ALL, ratesUnit, durationUnit);
        }
    }

    protected TSDBFileReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        }

    }
    // 1 RRD file per metric
    //

}
