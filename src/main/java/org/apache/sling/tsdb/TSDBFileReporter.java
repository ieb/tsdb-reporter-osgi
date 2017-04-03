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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by boston on 29/03/2017.
 */
public class TSDBFileReporter extends ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TSDBFileReporter.class);
    private final long[] recordPeriod;
    private final long[] blockPeriod;

    public static Builder forRegistry(MetricRegistry metricRegistry) {

        return new Builder(metricRegistry);
    }


    public static class Builder {

        private MetricRegistry metricRegistry;
        private TimeUnit ratesUnit;
        private TimeUnit durationUnit;
        private long[] recordPeriod;
        private long[] blockPeriod;

        public Builder(MetricRegistry metricRegistry ) {

            this.metricRegistry = metricRegistry;
        }

        public void setBlockPeriod(long[] blockPeriod) {
            this.blockPeriod = blockPeriod;
        }

        public void setRecordPeriod(long[] recordPeriod) {
            this.recordPeriod = recordPeriod;
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
            long[] blockPeriodIM = new long[blockPeriod.length];
            for( int i = 0; i < blockPeriod.length; i++) {
                blockPeriodIM[i] = blockPeriod[i];
            }
            long[] recordPeriodIM = new long[recordPeriod.length];
            for( int i = 0; i < recordPeriod.length; i++) {
                recordPeriodIM[i] = recordPeriod[i];
            }
            return new TSDBFileReporter(metricRegistry, "RRDReporter", MetricFilter.ALL, ratesUnit, durationUnit, recordPeriodIM, blockPeriodIM);
        }
    }

    protected TSDBFileReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, long[] recordPeriodIM, long[] blockPeriodIM) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.recordPeriod = recordPeriodIM;
        this.blockPeriod = blockPeriodIM;
    }
   
    

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

        long tnow = System.currentTimeMillis();
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            update(tnow, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            update(tnow, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            update(tnow, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            update(tnow, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            update(tnow, entry.getKey(), entry.getValue());
        }

    }
    // 1 RRD file per metric
    //

    private String toFileName(String name, String suffix) {
        return name+"-"+suffix+".tsdb";
    }
    
    private void update(long tnow, String name, Gauge g) {
        Object v = g.getValue();
        String vt = TSDBDef.getType(v);
        if ( vt != null ) {
            TSDBDef def = TSDBDef.builder()
                    .setFileName(toFileName(name, "g"))
                    .setFelds("l" + vt)
                    .setMergePolicy("vv")
                    .setName(name)
                    .setBlockPeriod(recordPeriod, blockPeriod)
                    .setMetadata("{ fields: [ 'timestamp', 'value' ] }")
                    .build();
            TSDBUpdater u = null;
            try {
                u = new TSDBUpdater(def);
                u.update(tnow,new Object[] { tnow, TSDBDef.getValue(v)});
            } catch (IOException e) {
                LOGGER.debug("Error Writing Metric ",e);
            } finally {
                if ( u != null) {
                    try {
                        u.close();
                    } catch (Exception e) {
                        LOGGER.debug("Cant close TSDBUpdater",e);
                    }
                }
            }
        }
    }

    private void update(long tnow, String name, Counter c) {
        TSDBDef def = TSDBDef.builder()
                .setFileName(toFileName(name, "g"))
                .setFelds("ll")
                .setMergePolicy("vv")
                .setName(name)
                .setBlockPeriod(blockPeriod, recordPeriod)
                .setMetadata("{ fields: [ 'timestamp', 'count' ] }")
                .build();
        TSDBUpdater u = null;
        try {
            u = new TSDBUpdater(def);
            u.update(tnow,new Object[] { tnow, c.getCount()});
        } catch (IOException e) {
            LOGGER.debug("Error Writing Metric ",e);
        } finally {
            if ( u != null) {
                try {
                    u.close();
                } catch (Exception e) {
                    LOGGER.debug("Cant close TSDBUpdater",e);
                }
            }
        }
    }

    private void update(long tnow, String name, Histogram h) {
        TSDBDef def = TSDBDef.builder()
                .setFileName(toFileName(name, "g"))
                .setFelds("llllddddddddl")
                .setMergePolicy("vvlummmmmmmmv")
                .setName(name)
                .setBlockPeriod(blockPeriod, recordPeriod)
                .setMetadata("{ fields: [ 'timestamp', " +
                        "'count', " +
                        "'min', " +
                        "'max', " +
                        "'mean', " +
                        "'std-dev', " +
                        "'50-percentile', " +
                        "'75-percentile', " +
                        "'95-percentile', " +
                        "'99-percentile', " +
                        "'999-percentile', " +
                        "'run-count' ] }")
                .build();
        TSDBUpdater u = null;
        try {
            u = new TSDBUpdater(def);
            Snapshot s = h.getSnapshot();
            u.update(tnow,new Object[] { tnow, //lv
                    s.size(),       //lv
                    convertDuration(s.getMin()),    //ll
                    convertDuration(s.getMax()),    //lu
                    convertDuration(s.getMean()),   //dm
                    convertDuration(s.getStdDev()), //dm
                    convertDuration(s.getMedian()), //dm
                    convertDuration(s.get75thPercentile()),  // dm
                    convertDuration(s.get95thPercentile()),  // dm
                    convertDuration(s.get98thPercentile()),  // dm
                    convertDuration(s.get99thPercentile()),  // dm
                    convertDuration(s.get999thPercentile()), // dm
                    h.getCount() //lv
            });
            
        } catch (IOException e) {
            LOGGER.debug("Error Writing Metric ",e);
        } finally {
            if ( u != null) {
                try {
                    u.close();
                } catch (Exception e) {
                    LOGGER.debug("Cant close TSDBUpdater",e);
                }
            }
        }
    }

    private void update(long tnow, String name, Timer t) {
        TSDBDef def = TSDBDef.builder()
                .setFileName(toFileName(name, "g"))
                .setFelds("llllddddddddddddl")
                .setMergePolicy("vvlummmmmmmmmmmmv")
                .setName(name)
                .setBlockPeriod(blockPeriod, recordPeriod)
                .setMetadata("{ fields: [ 'timestamp', " +
                        "'count', " +
                        "'min', " +
                        "'max', " +
                        "'mean', " +
                        "'std-dev', " +
                        "'50-percentile', " +
                        "'75-percentile', " +
                        "'95-percentile', " +
                        "'99-percentile', " +
                        "'999-percentile', " +
                        "'one-minute', " +
                        "'five-minute', " +
                        "'fifteen-minute', " +
                        "'mean-minute'" +
                        "'run-count' ] }")
                .build();
        TSDBUpdater u = null;
        try {
            u = new TSDBUpdater(def);
            Snapshot s = t.getSnapshot();
            u.update(tnow,new Object[] { tnow, //lv
                    s.size(),       //lv
                    convertDuration(s.getMin()),    //ll
                    convertDuration(s.getMax()),    //lu
                    convertDuration(s.getMean()),   //dm
                    convertDuration(s.getStdDev()), //dm
                    convertDuration(s.getMedian()), //dm
                    convertDuration(s.get75thPercentile()),  // dm
                    convertDuration(s.get95thPercentile()),  // dm
                    convertDuration(s.get98thPercentile()),  // dm
                    convertDuration(s.get99thPercentile()),  // dm
                    convertDuration(s.get999thPercentile()), // dm
                    convertRate(t.getOneMinuteRate()),  //d
                    convertRate(t.getFiveMinuteRate()), //d
                    convertRate(t.getFifteenMinuteRate()), //d
                    convertRate(t.getMeanRate()), //d
                    t.getCount() //lv
            });

        } catch (IOException e) {
            LOGGER.debug("Error Writing Metric ",e);
        } finally {
            if ( u != null) {
                try {
                    u.close();
                } catch (Exception e) {
                    LOGGER.debug("Cant close TSDBUpdater",e);
                }
            }
        }
    }


    private void update(long tnow, String name, Meter m) {
        TSDBDef def = TSDBDef.builder()
                .setFileName(toFileName(name, "g"))
                .setFelds("lldddd")
                .setMergePolicy("vvmmmm")
                .setName(name)
                .setBlockPeriod(blockPeriod, recordPeriod)
                .setMetadata("{ fields: [ 'timestamp', " +
                        "'count', " +
                        "'one-minute', " +
                        "'five-minute', " +
                        "'fifteen-minute', " +
                        "'mean-minute' ] }")
                .build();
        TSDBUpdater u = null;
        try {
            u = new TSDBUpdater(def);
            u.update(tnow,new Object[] { tnow, //lv
                    m.getCount(), // lv
                    convertRate(m.getOneMinuteRate()),  //d
                    convertRate(m.getFiveMinuteRate()), //d
                    convertRate(m.getFifteenMinuteRate()), //d
                    convertRate(m.getMeanRate()), //d
            });

        } catch (IOException e) {
            LOGGER.debug("Error Writing Metric ",e);
        } finally {
            if ( u != null) {
                try {
                    u.close();
                } catch (Exception e) {
                    LOGGER.debug("Cant close TSDBUpdater",e);
                }
            }
        }
    }
}
