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
import org.apache.felix.scr.annotations.*;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a shim class so the bundle works with versions of Oak that didnt register the MetricsRegistry as a service.
 * There may be issues with Sling which for a time created its own independent MetricsRegistry.
 */
// @Component(immediate = true)
public class OakStatisticsProviderShim {
    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY, policy = ReferencePolicy.STATIC, policyOption = ReferencePolicyOption.GREEDY)
    private MetricRegistry metricsRegistry;

    @Reference
    private StatisticsProvider statisticsProvider;
    private List<ServiceRegistration> regs = new ArrayList<ServiceRegistration>();


    @Activate
    private void activate(BundleContext bundleContext, Map<String, Object> config) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (metricsRegistry == null) { // no metrics registry, so assume the StatisticsProvider didnt register it, fix that.
            regs.add(bundleContext.registerService(MetricRegistry.class.getName(), getMetricsRegistry(statisticsProvider),null));
        }
    }

    private MetricRegistry getMetricsRegistry(StatisticsProvider statisticsProvider) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = statisticsProvider.getClass().getDeclaredMethod("getRegistry");
        if (!m.isAccessible()) {
            m.setAccessible(true);
        }
        return (MetricRegistry) m.invoke(statisticsProvider);
    }


    @Deactivate
    private void deactivate(BundleContext bundleContext, Map<String, Object> config) {
        for (ServiceRegistration r : regs) {
            r.unregister();
        }
        regs.clear();
    }
}
