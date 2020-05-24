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

import java.util.Map;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import com.google.auto.value.AutoValue;

import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.CassandraConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;

@AutoValue
public abstract class CassandraFeatureSink implements FeatureSink {

	/**
	 * Initialize a {@link CassandraFeatureSink.Builder} from a
	 * {@link StoreProto.Store.CassandraConfig}.
	 *
	 * @param cassandraConfig {@link CassandraConfig}
	 * @param featureSetSpecs
	 * @return {@link CassandraFeatureSink.Builder}
	 */
	public static FeatureSink fromConfig(CassandraConfig cassandraConfig, Map<String, FeatureSetSpec> featureSetSpecs) {
		return builder().setFeatureSetSpecs(featureSetSpecs).setCassandraConfig(cassandraConfig).build();
	}

	public abstract CassandraConfig getCassandraConfig();

	public abstract Map<String, FeatureSetSpec> getFeatureSetSpecs();

	public abstract Builder toBuilder();

	public static Builder builder() {
		return new AutoValue_CassandraFeatureSink.Builder();
	}

	@AutoValue.Builder
	public abstract static class Builder {
		public abstract Builder setCassandraConfig(CassandraConfig cassandraConfig);

		public abstract Builder setFeatureSetSpecs(Map<String, FeatureSetSpec> featureSetSpecs);

		public abstract CassandraFeatureSink build();
	}

	@Override
	public void prepareWrite(FeatureSet featureSet) {
		CassandraStore.validateStore(getCassandraConfig());
	}

	@Override
	public PTransform<PCollection<FeatureRow>, WriteResult> writer() {
		return new CassandraCustomIO.Write(getCassandraConfig(), getFeatureSetSpecs());
	}
}
