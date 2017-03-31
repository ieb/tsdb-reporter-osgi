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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.apache.felix.scr.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by boston on 29/03/2017.
 */
@Component(immediate = true)
@References(value = {@Reference(
        referenceInterface = MetricRegistry.class,
        cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
        policy = ReferencePolicy.DYNAMIC,
        bind = "bindMetricRegistry",
        unbind = "unbindMetricRegistry")}
)
public class CodehaleMetricsReporterComponent {

    private static final Logger LOG = LoggerFactory.getLogger(CodehaleMetricsReporterComponent.class);

    private ScheduledReporter reporter;


    private ConcurrentMap<String, CopyMetricRegistryListener> listeners = new ConcurrentHashMap<String, CopyMetricRegistryListener>();
    private MetricRegistry metricRegistry = new MetricRegistry();

    @Activate
    public void acivate(Map<String, Object> properties) throws IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        LOG.info("Starting RRD Metrics reporter ");
        reporter = TSDBFileReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .tag("hostname", getHostName())
                .build();
        reporter.start(5, TimeUnit.SECONDS);
        LOG.info("Started RRD Metrics reporter ");
    }

    @Deactivate
    public void deacivate(Map<String, Object> properties) {
        reporter.stop();
        reporter = null;
    }

    protected void bindMetricRegistry(MetricRegistry metricRegistry, Map<String, Object> properties) {
        String name = (String) properties.get("name");
        if (name == null) {
            name = metricRegistry.toString();
        }
        CopyMetricRegistryListener listener = new CopyMetricRegistryListener(this.metricRegistry, name);
        listener.start(metricRegistry);
        this.listeners.put(name, listener);
        LOG.info("Bound Metrics Registry {} ",name);
    }
    protected void unbindMetricRegistry(MetricRegistry metricRegistry, Map<String, Object> properties) {
        String name = (String) properties.get("name");
        if (name == null) {
            name = metricRegistry.toString();
        }
        CopyMetricRegistryListener metricRegistryListener = listeners.get(name);
        if ( metricRegistryListener != null) {
            metricRegistryListener.stop(metricRegistry);
            this.listeners.remove(name);
        }
        LOG.info("Unbound Metrics Registry {} ",name);
    }

    public String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch ( Exception ex ) {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "Unknown ip";
            }
        }
    }
}
