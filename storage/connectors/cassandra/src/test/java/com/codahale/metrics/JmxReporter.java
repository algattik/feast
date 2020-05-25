/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codahale.metrics;

import com.codahale.metrics.jmx.ObjectNameFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;

/**
 * Shim for {@link com.codahale.metrics.jmx.JmxReporter} provided in package {@link
 * com.codahale.metrics}. Workaround for the fact that the Cassandra driver relies on Metrics 3,
 * while Spring Boot replies on Metrics 4, and {@link org.apache.beam.sdk.io.cassandra.CassandraIO}
 * does not support the {@link com.datastax.driver.core.Cluster.Builder#withoutJMXReporting()}
 * setting.
 *
 * @see <a href=
 *     "https://docs.datastax.com/en/developer/java-driver/3.7/manual/metrics/#metrics-4-compatibility">Cassandra
 *     Driver Metrics 4 Compatibility</a>
 */
public class JmxReporter {

  com.codahale.metrics.jmx.JmxReporter proxied;

  public JmxReporter(com.codahale.metrics.jmx.JmxReporter proxied) {
    this.proxied = proxied;
  }

  public static Builder forRegistry(MetricRegistry registry) {
    Builder builder = new Builder();
    builder.proxied = com.codahale.metrics.jmx.JmxReporter.forRegistry(registry);
    return builder;
  }

  public void start() {
    proxied.start();
  }

  public void stop() {
    proxied.stop();
  }

  public static class Builder {

    com.codahale.metrics.jmx.JmxReporter.Builder proxied;

    public Builder registerWith(MBeanServer mBeanServer) {
      proxied.registerWith(mBeanServer);
      return this;
    }

    public Builder convertRatesTo(TimeUnit rateUnit) {
      proxied.convertRatesTo(rateUnit);
      return this;
    }

    public Builder createsObjectNamesWith(ObjectNameFactory onFactory) {
      proxied.createsObjectNamesWith(onFactory);
      return this;
    }

    public Builder convertDurationsTo(TimeUnit durationUnit) {
      proxied.convertDurationsTo(durationUnit);
      return this;
    }

    public Builder filter(MetricFilter filter) {
      proxied.filter(filter);
      return this;
    }

    public Builder inDomain(String domain) {
      proxied.inDomain(domain);
      return this;
    }

    public Builder specificDurationUnits(Map<String, TimeUnit> specificDurationUnits) {
      proxied.specificDurationUnits(specificDurationUnits);
      return this;
    }

    public Builder specificRateUnits(Map<String, TimeUnit> specificRateUnits) {
      proxied.specificRateUnits(specificRateUnits);
      return this;
    }

    public JmxReporter build() {
      return new JmxReporter(proxied.build());
    }
  }
}
