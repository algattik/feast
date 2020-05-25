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
package feast.storage.connectors.cassandra.writer;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto.Store.CassandraConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraCustomIO {

  static TupleTag<CassandraMutation> mutationsTag = new TupleTag<CassandraMutation>("mutations") {};
  static TupleTag<FeatureRow> successfulInsertsTag =
      new TupleTag<FeatureRow>("successfulInserts") {};
  static TupleTag<FailedElement> failedInsertsTupleTag =
      new TupleTag<FailedElement>("failedInserts") {};

  private static final Logger log = LoggerFactory.getLogger(CassandraCustomIO.class);

  private CassandraCustomIO() {}

  public static Write write(
      CassandraConfig cassandraConfig, Map<String, FeatureSetSpec> featureSetSpecs) {
    return new Write(cassandraConfig, featureSetSpecs);
  }

  /** ServingStoreWrite data to a Cassandra server. */
  public static class Write extends PTransform<PCollection<FeatureRow>, WriteResult> {

    private Map<String, FeatureSetSpec> featureSetSpecs;
    private CassandraConfig cassandraConfig;

    public Write(CassandraConfig cassandraConfig, Map<String, FeatureSetSpec> featureSetSpecs) {

      this.cassandraConfig = cassandraConfig;
      this.featureSetSpecs = featureSetSpecs;
    }

    @Override
    public WriteResult expand(PCollection<FeatureRow> input) {

      CassandraMutationMapperFactory mapperFactory =
          new CassandraMutationMapperFactory(CassandraMutation.class);

      PCollectionTuple cassandraWrite =
          input.apply(
              ParDo.of(new WriteDoFn(featureSetSpecs, cassandraConfig.getDefaultTtl()))
                  .withOutputTags(
                      mutationsTag,
                      TupleTagList.of(successfulInsertsTag).and(failedInsertsTupleTag)));

      cassandraWrite
          .get(mutationsTag)
          .apply(
              CassandraIO.<CassandraMutation>write()
                  .withHosts(Arrays.asList(cassandraConfig.getBootstrapHosts().split(",")))
                  .withPort(cassandraConfig.getPort())
                  .withKeyspace(cassandraConfig.getKeyspace())
                  .withEntity(CassandraMutation.class)
                  .withMapperFactoryFn(mapperFactory)
                  .withConsistencyLevel(String.valueOf(ConsistencyLevel.ALL)));

      return WriteResult.in(
          input.getPipeline(),
          cassandraWrite.get(successfulInsertsTag),
          cassandraWrite.get(failedInsertsTupleTag));
    }
  }

  public static class WriteDoFn extends DoFn<FeatureRow, CassandraMutation> {

    private final Map<String, FeatureSetSpec> featureSetSpecs;
    private final Map<String, Integer> maxAges;

    public WriteDoFn(Map<String, FeatureSetSpec> featureSetSpecs, Duration defaultTtl) {
      this.featureSetSpecs = featureSetSpecs;
      this.maxAges = new HashMap<>();
      for (FeatureSetSpec spec : featureSetSpecs.values()) {
        String featureSetRef = String.format("%s/%s", spec.getProject(), spec.getName());
        if (spec.getMaxAge() != null && spec.getMaxAge().getSeconds() > 0) {
          maxAges.put(featureSetRef, Math.toIntExact(spec.getMaxAge().getSeconds()));
        } else {
          maxAges.put(featureSetRef, Math.toIntExact(defaultTtl.getSeconds()));
        }
      }
    }

    /** Output a Cassandra mutation object for every feature in the feature row. */
    @ProcessElement
    public void processElement(ProcessContext context) {
      FeatureRow featureRow = context.element();
      try {
        FeatureSetSpec featureSetSpec = featureSetSpecs.get(featureRow.getFeatureSet());
        Set<String> featureNames =
            featureSetSpec.getFeaturesList().stream()
                .map(FeatureSpec::getName)
                .collect(Collectors.toSet());
        String key = CassandraMutation.keyFromFeatureRow(featureSetSpec, featureRow);

        for (Field field : featureRow.getFieldsList()) {
          if (featureNames.contains(field.getName())) {
            context.output(
                mutationsTag,
                new CassandraMutation(
                    key,
                    field.getName(),
                    ByteBuffer.wrap(field.getValue().toByteArray()),
                    Timestamps.toMicros(featureRow.getEventTimestamp()),
                    maxAges.get(featureRow.getFeatureSet())));
          }
        }

        context.output(successfulInsertsTag, featureRow);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        log.error(maxAges.toString());
        log.error(featureRow.getFeatureSet());
        FailedElement failedElement =
            toFailedElement(featureRow, e, context.getPipelineOptions().getJobName());
        context.output(failedInsertsTupleTag, failedElement);
      }
    }

    private FailedElement toFailedElement(
        FeatureRow featureRow, Exception exception, String jobName) {
      return FailedElement.newBuilder()
          .setJobName(jobName)
          .setTransformName("CassandraCustomIO")
          .setPayload(featureRow.toString())
          .setErrorMessage(exception.getMessage())
          .setStackTrace(ExceptionUtils.getStackTrace(exception))
          .build();
    }
  }
}
