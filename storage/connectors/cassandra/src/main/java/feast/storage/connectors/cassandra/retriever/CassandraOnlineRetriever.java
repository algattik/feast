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
package feast.storage.connectors.cassandra.retriever;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;

import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto.Store.CassandraConfig;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetJobRequest;
import feast.proto.serving.ServingAPIProto.GetJobResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import feast.storage.connectors.cassandra.common.ValueUtil;
import io.grpc.Status;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.prometheus.client.Histogram;

public class CassandraOnlineRetriever implements OnlineRetriever {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(CassandraOnlineRetriever.class);

	private final Session session;
	private final String keyspace;
	private final String tableName;
	private final Tracer tracer;
	private final PreparedStatement query;
	private final Histogram requestLatency;

	public CassandraOnlineRetriever(Session session, String keyspace, String tableName, Tracer tracer,
			Histogram requestLatency) {
		super();
		this.session = session;
		this.keyspace = keyspace;
		this.tableName = tableName;
		this.tracer = tracer;
		this.query = session.prepare(String.format(
				"SELECT entities, feature, value, WRITETIME(value) as writetime FROM %s.%s WHERE entities = ?",
				keyspace, tableName));
		this.requestLatency = requestLatency;
	}

	public static OnlineRetriever create(Map<String, String> config, Tracer tracer, Histogram requestLatency) {
		CassandraConfig cassandraConfig = CassandraConfig.newBuilder().setBootstrapHosts(config.get("host"))
				.setPort(Integer.parseInt(config.get("port"))).setKeyspace(config.get("keyspace")).build();

		List<InetSocketAddress> contactPoints = Arrays.stream(cassandraConfig.getBootstrapHosts().split(","))
				.map(h -> new InetSocketAddress(h, cassandraConfig.getPort())).collect(Collectors.toList());
		Cluster cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();
		// Session in Cassandra is thread-safe and maintains connections to cluster
		// nodes internally
		// Recommended to use one session per keyspace instead of open and close
		// connection for each
		// request
		log.info(String.format("Cluster name: %s", cluster.getClusterName()));
		log.info(String.format("Cluster keyspaces: %s", cluster.getMetadata().getKeyspaces()));
		log.info(String.format("Cluster nodes: %s", cluster.getMetadata().getAllHosts()));
		log.info(String.format("Cluster token ranges: %s", cluster.getMetadata().getTokenRanges()));

		Session session = cluster.connect();

		return new CassandraOnlineRetriever(session, cassandraConfig.getKeyspace(), cassandraConfig.getTableName(),
				tracer, requestLatency);
	}

	@Override
	public List<List<FeatureRow>> getOnlineFeatures(List<EntityRow> entityRows,
			List<FeatureSetRequest> featureSetRequests) {

		List<List<FeatureRow>> responses = new ArrayList<List<FeatureRow>>(featureSetRequests.size());
		for (FeatureSetRequest featureSetRequest : featureSetRequests) {

			List<String> featureSetEntityNames = featureSetRequest.getSpec().getEntitiesList().stream()
					.map(EntitySpec::getName).collect(Collectors.toList());

			log.info("Computiing cassandra keys from {} with rows {}", featureSetEntityNames, entityRows);
			List<String> cassandraKeys = createLookupKeys(featureSetEntityNames, entityRows, featureSetRequest);
			try {
				responses.add(getAndProcessAll(cassandraKeys, entityRows, featureSetRequest));
			} catch (Exception e) {
				log.info(e.getStackTrace().toString());
				throw Status.INTERNAL.withDescription("Unable to parse cassandra response/ while retrieving feature")
						.withCause(e).asRuntimeException();
			}
		}
		return responses;
	}

	List<String> createLookupKeys(List<String> featureSetEntityNames, List<EntityRow> entityRows,
			FeatureSetRequest featureSetRequest) {
		try (Scope scope = tracer.buildSpan("Cassandra-makeCassandraKeys").startActive(true)) {
			FeatureSetSpec fsSpec = featureSetRequest.getSpec();
			String featureSetId = String.format("%s/%s", fsSpec.getProject(), fsSpec.getName());
			return entityRows.stream().map(row -> createCassandraKey(featureSetId, featureSetEntityNames, row))
					.collect(Collectors.toList());
		}
	}

	/**
	 * Send a list of get request as an mget
	 *
	 * @param keys list of string keys
	 */
	protected List<FeatureRow> getAndProcessAll(List<String> keys, List<EntityRow> entityRows,
			FeatureSetRequest featureSetRequest) {
		FeatureSetSpec spec = featureSetRequest.getSpec();
		log.debug("Sending multi get: {}", keys);
		List<ResultSet> results = sendMultiGet(keys);
		long startTime = System.currentTimeMillis();
		for (String key : keys) {
			log.info("Looking up for key {}", key);
			results.add(session.execute(
					QueryBuilder.select().column("entities").column("feature").column("value").writeTime("value")
							.as("writetime").from(keyspace, tableName).where(QueryBuilder.eq("entities", key))));
		}
		try (Scope scope = tracer.buildSpan("Cassandra-processResponse").startActive(true)) {
			List<FeatureRow> response = new ArrayList<FeatureRow>();
			for (int i = 0; i < results.size(); i++) {
				EntityRow entityRow = entityRows.get(i);
				ResultSet queryRows = results.get(i);
				Instant instant = Instant.now();
				List<Field> fields = new ArrayList<>();
				while (!queryRows.isExhausted()) {
					Row row = queryRows.one();
					if (row.isNull("writetime")) {
						log.info("The row in question doesn't have a write time column {}", row.getColumnDefinitions());
						log.info("Bad row query returned: {}", row);
						log.info("Entities {}, Key {}", entityRow, keys.get(i));
						continue;
					}
					long microSeconds = row.getLong("writetime");
					instant = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(microSeconds),
							TimeUnit.MICROSECONDS.toNanos(Math.floorMod(microSeconds, TimeUnit.SECONDS.toMicros(1))));
					try {
						fields.add(Field.newBuilder().setName(row.getString("feature"))
								.setValue(Value.parseFrom(ByteBuffer.wrap(row.getBytes("value").array()))).build());
					} catch (InvalidProtocolBufferException e) {
						throw new RuntimeException(e);
					}
					FeatureRow featureRow = FeatureRow.newBuilder().addAllFields(fields).setEventTimestamp(Timestamp
							.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build())
							.build();
					response.add(featureRow);
				}
			}
			return response;
		} finally {
			requestLatency.labels("processResponse").observe((System.currentTimeMillis() - startTime) / 1000);
		}
	}

	/**
	 * Create cassandra keys
	 *
	 * @param featureSet            featureSet reference of the feature. E.g.
	 *                              feature_set_1:1
	 * @param featureSetEntityNames entity names that belong to the featureSet
	 * @param entityRow             entityRow to build the key from
	 * @return String
	 */
	private static String createCassandraKey(String featureSet, List<String> featureSetEntityNames,
			EntityRow entityRow) {
		Map<String, Value> fieldsMap = entityRow.getFieldsMap();
		List<String> res = new ArrayList<>();
		for (String entityName : featureSetEntityNames) {
			res.add(entityName + "=" + ValueUtil.toString(fieldsMap.get(entityName)));
		}
		return featureSet + ":" + String.join("|", res);
	}

	/**
	 * Send a list of get request as an cassandra execution
	 *
	 * @param keys list of cassandra keys
	 * @return list of {@link FeatureRow} in cassandra representation for each
	 *         cassandra keys
	 */
	private List<ResultSet> sendMultiGet(List<String> keys) {
		try (Scope scope = tracer.buildSpan("Cassandra-sendMultiGet").startActive(true)) {
			List<ResultSet> results = new ArrayList<>();
			long startTime = System.currentTimeMillis();
			try {
				for (String key : keys) {
					results.add(
							session.execute(query.bind(key).enableTracing().setConsistencyLevel(ConsistencyLevel.TWO)));
				}
				return results;
			} catch (Exception e) {
				throw Status.NOT_FOUND.withDescription("Unable to retrieve feature from Cassandra").withCause(e)
						.asRuntimeException();
			} finally {
				requestLatency.labels("sendMultiGet").observe((System.currentTimeMillis() - startTime) / 1000d);
			}
		}
	}

	// FIXME duplicated from RefUtil
	public static String generateFeatureStringRef(FeatureReference featureReference) {
		String ref = featureReference.getName();
		if (!featureReference.getFeatureSet().isEmpty()) {
			ref = featureReference.getFeatureSet() + ":" + ref;
		}
		if (!featureReference.getProject().isEmpty()) {
			ref = featureReference.getProject() + "/" + ref;
		}
		return ref;
	}

}