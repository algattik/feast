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
package feast.storage.connectors.cassandra.writer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.MappingManager;
import feast.proto.core.StoreProto.Store.CassandraConfig;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public abstract class CassandraStore {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(CassandraStore.class);

  /**
   * Ensures Cassandra is accessible, else throw a RuntimeException. Creates Cassandra keyspace and
   * table if it does not already exist
   *
   * @param cassandraConfig Please refer to feast.core.Store proto
   */
  public static void validateStore(CassandraConfig cassandraConfig) {
    List<InetSocketAddress> contactPoints =
        Arrays.stream(cassandraConfig.getBootstrapHosts().split(","))
            .map(host -> new InetSocketAddress(host, cassandraConfig.getPort()))
            .collect(Collectors.toList());
    Cluster cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();
    Session session;

    try {
      String keyspace = cassandraConfig.getKeyspace();
      KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
      if (keyspaceMetadata == null) {
        log.info("Creating keyspace '{}'", keyspace);
        Map<String, Object> replicationOptions =
            cassandraConfig.getReplicationOptions().getAllFields().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getName(), Entry::getValue));
        KeyspaceOptions createKeyspace =
            SchemaBuilder.createKeyspace(keyspace)
                .ifNotExists()
                .with()
                .replication(replicationOptions);
        session = cluster.newSession();
        session.execute(createKeyspace);
      }

      session = cluster.connect(keyspace);
      // Currently no support for creating table from entity mapper:
      // https://datastax-oss.atlassian.net/browse/JAVA-569
      Create createTable =
          SchemaBuilder.createTable(keyspace, cassandraConfig.getTableName())
              .ifNotExists()
              .addPartitionKey(CassandraMutation.ENTITIES, DataType.text())
              .addClusteringColumn(CassandraMutation.FEATURE, DataType.text())
              .addColumn(CassandraMutation.VALUE, DataType.blob());
      log.info("Create Cassandra table if not exists..");
      session.execute(createTable);

      validateCassandraTable(session);

      session.close();
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to Cassandra at bootstrap hosts: '%s' port: '%s'. Please check that your Cassandra is running and accessible from Feast.",
              contactPoints.stream()
                  .map(InetSocketAddress::getHostName)
                  .collect(Collectors.joining(",")),
              cassandraConfig.getPort()),
          e);
    }
    cluster.close();
  }

  private static void validateCassandraTable(Session session) {
    try {
      new MappingManager(session).mapper(CassandraMutation.class).getTableMetadata();
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Table created does not match the datastax object mapper: %s",
              CassandraMutation.class.getSimpleName()));
    }
  }
}
