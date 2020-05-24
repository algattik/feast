/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.storage.connectors.cassandra.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.google.protobuf.Timestamp;

import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto.Store.CassandraConfig;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.storage.connectors.cassandra.writer.CassandraStore;

@SuppressWarnings("WeakerAccess")
public class TestUtil {

	public static class LocalCassandra {

		public static void start() throws InterruptedException, IOException, TTransportException {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		}

		public static void createKeyspaceAndTable(CassandraConfig config) {
			TestUtil.setupCassandra(config);
		}

		public static String getHost() {
			return EmbeddedCassandraServerHelper.getHost();
		}

		public static int getPort() {
			return EmbeddedCassandraServerHelper.getNativeTransportPort();
		}

		public static Cluster getCluster() {
			return EmbeddedCassandraServerHelper.getCluster();
		}

		public static Session getSession() {
			return EmbeddedCassandraServerHelper.getSession();
		}

		public static void stop() {
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		}
	}

	/**
	 * Create a Feature Set Spec.
	 *
	 * @param name          name of the feature set
	 * @param version       version of the feature set
	 * @param maxAgeSeconds max age
	 * @param entities      entities provided as map of string to {@link Enum}
	 * @param features      features provided as map of string to {@link Enum}
	 * @return {@link FeatureSetSpec}
	 */
	@Deprecated
	public static FeatureSetSpec createFeatureSetSpec(String name, String project, int version, int maxAgeSeconds,
			Map<String, Enum> entities, Map<String, Enum> features) {
		return createFeatureSetSpec(name, project, maxAgeSeconds, entities, features);
	}
	
	/**
	 * Create a Feature Set Spec.
	 *
	 * @param name          name of the feature set
	 * @param maxAgeSeconds max age
	 * @param entities      entities provided as map of string to {@link Enum}
	 * @param features      features provided as map of string to {@link Enum}
	 * @return {@link FeatureSetSpec}
	 */
	public static FeatureSetSpec createFeatureSetSpec(String name, String project, int maxAgeSeconds,
			Map<String, Enum> entities, Map<String, Enum> features) {
		FeatureSetSpec.Builder featureSetSpec = FeatureSetSpec.newBuilder().setName(name).setProject(project)
				.setMaxAge(com.google.protobuf.Duration.newBuilder().setSeconds(maxAgeSeconds).build());

		for (Entry<String, Enum> entity : entities.entrySet()) {
			featureSetSpec.addEntities(
					EntitySpec.newBuilder().setName(entity.getKey()).setValueType(entity.getValue()).build());
		}

		for (Entry<String, Enum> feature : features.entrySet()) {
			featureSetSpec.addFeatures(
					FeatureSpec.newBuilder().setName(feature.getKey()).setValueType(feature.getValue()).build());
		}

		return featureSetSpec.build();
	}

	/**
	 * Create a Feature Row.
	 *
	 * @param featureSetSpec   {@link FeatureSetSpec}
	 * @param timestampSeconds timestamp given in seconds
	 * @param fields           fields provided as a map name to {@link Value}
	 * @return {@link FeatureRow}
	 */
	public static FeatureRow createFeatureRow(FeatureSetSpec featureSetSpec, long timestampSeconds,
			Map<String, Value> fields) {
		List<String> featureNames = featureSetSpec.getFeaturesList().stream().map(FeatureSpec::getName)
				.collect(Collectors.toList());
		List<String> entityNames = featureSetSpec.getEntitiesList().stream().map(EntitySpec::getName)
				.collect(Collectors.toList());
		List<String> requiredFields = Stream.concat(featureNames.stream(), entityNames.stream())
				.collect(Collectors.toList());

		if (fields.keySet().containsAll(requiredFields)) {
			FeatureRow.Builder featureRow = FeatureRow.newBuilder()
					.setFeatureSet(featureSetSpec.getProject() + "/" + featureSetSpec.getName())
					.setEventTimestamp(Timestamp.newBuilder().setSeconds(timestampSeconds).build());
			for (Entry<String, Value> field : fields.entrySet()) {
				featureRow.addFields(Field.newBuilder().setName(field.getKey()).setValue(field.getValue()).build());
			}
			return featureRow.build();
		} else {
			String missingFields = requiredFields.stream().filter(f -> !fields.keySet().contains(f))
					.collect(Collectors.joining(","));
			throw new IllegalArgumentException(
					"FeatureRow is missing some fields defined in FeatureSetSpec: " + missingFields);
		}
	}

	public static List<Map<String, Value>> responseToMapList(GetOnlineFeaturesResponse response) {
		return response.getFieldValuesList().stream().map(FieldValues::getFieldsMap).collect(Collectors.toList());
	}

	public static Value intValue(int val) {
		return Value.newBuilder().setInt64Val(val).build();
	}

	public static Value strValue(String val) {
		return Value.newBuilder().setStringVal(val).build();
	}

	public static void setupCassandra(CassandraConfig config) {
		CassandraStore.validateStore(config);
	}
}
