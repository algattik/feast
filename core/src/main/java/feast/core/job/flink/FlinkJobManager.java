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
package feast.core.job.flink;

import static feast.core.util.PipelineUtil.detectClassPathResourcesToStage;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.job.option.FeatureSetJsonByteConverter;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Project;
import feast.core.util.TypeConversion;
import feast.ingestion.ImportJob;
import feast.ingestion.options.BZip2Compressor;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.options.OptionCompressor;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.FlinkRunnerResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.GlobalConfiguration;
import org.springframework.web.client.RestTemplate;

@Slf4j
public class FlinkJobManager implements JobManager {

  private final Runner RUNNER_TYPE = Runner.FLINK;

  private final Map<String, String> defaultOptions;
  private final MetricsProperties metrics;
  private final CliFrontend flinkCli;
  private final FlinkRestApi flinkRestApis;
  private final FlinkRunnerConfig config;

  public FlinkJobManager(
      Map<String, String> runnerConfigOptions, MetricsProperties metricsProperties) {

    config =
        new FlinkRunnerConfig(
            runnerConfigOptions.get("masterUrl"), runnerConfigOptions.get("configDir"));

    org.apache.flink.configuration.Configuration configuration =
        GlobalConfiguration.loadConfiguration(config.getConfigDir());
    List<CustomCommandLine<?>> customCommandLines =
        CliFrontend.loadCustomCommandLines(configuration, config.getConfigDir());
    try {
      this.flinkCli = new CliFrontend(configuration, customCommandLines);
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new IllegalArgumentException("Can't construct CliFrontend");
    }
    this.flinkRestApis = new FlinkRestApi(new RestTemplate(), config.getMasterUrl());

    this.defaultOptions = Map.of("flinkMaster", runnerConfigOptions.get("masterUrl"));
    this.metrics = metricsProperties;
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  @Override
  public Job startJob(Job job) {
    try {
      List<FeatureSetProto.FeatureSet> featureSetProtos = new ArrayList<>();
      for (FeatureSet featureSet : job.getFeatureSets()) {
        featureSetProtos.add(featureSet.toProto());
      }
      String extId =
          submitFlinkJob(
              job.getId(),
              featureSetProtos,
              job.getSource().toProto(),
              job.getStore().toProto(),
              false);
      job.setExtId(extId);
      return job;

    } catch (InvalidProtocolBufferException e) {
      log.error(e.getMessage());
      throw new IllegalArgumentException(
          String.format(
              "FlinkJobManager failed to START job with id '%s' because the job"
                  + "has an invalid spec. Please check the FeatureSet, Source and Store specs. Actual error message: %s",
              job.getId(), e.getMessage()));
    }
  }

  /**
   * Update an existing Flink job.
   *
   * @param job job of target job to change
   * @return Flink-specific job id
   */
  @Override
  public Job updateJob(Job job) {
    try {
      List<FeatureSetProto.FeatureSet> featureSetProtos = new ArrayList<>();
      for (FeatureSet featureSet : job.getFeatureSets()) {
        featureSetProtos.add(featureSet.toProto());
      }

      String extId =
          submitFlinkJob(
              job.getId(),
              featureSetProtos,
              job.getSource().toProto(),
              job.getStore().toProto(),
              true);

      job.setExtId(extId);
      job.setStatus(JobStatus.PENDING);
      return job;
    } catch (InvalidProtocolBufferException e) {
      log.error(e.getMessage());
      throw new IllegalArgumentException(
          String.format(
              "FlinkJobManager failed to UPDATE job with id '%s' because the job"
                  + "has an invalid spec. Please check the FeatureSet, Source and Store specs. Actual error message: %s",
              job.getId(), e.getMessage()));
    }
  }

  /**
   * Abort an existing Flink job. Streaming Flink jobs are always drained, not cancelled.
   *
   * @param flinkJobId Flink-specific job id (not the job name)
   */
  @Override
  public void abortJob(String flinkJobId) {
    flinkCli.parseParameters(createStopArgs(flinkJobId));
  }

  /**
   * Restart a Flink job. Flink should ensure continuity such that no data should be lost during the
   * restart operation.
   *
   * @param job job to restart
   * @return the restarted job
   */
  @Override
  public Job restartJob(Job job) {
    if (job.getStatus().isTerminal()) {
      // job yet not running: just start job
      return this.startJob(job);
    } else {
      // job is running - updating the job without changing the job has
      // the effect of restarting the job
      return this.updateJob(job);
    }
  }

  /**
   * Get status of a flink job with given id and try to map it into Feast's JobStatus.
   *
   * @param job Job containing flink job id
   * @return status of the job, or return {@link JobStatus#UNKNOWN} if error happens.
   */
  @Override
  public JobStatus getJobStatus(Job job) {
    if (job.getRunner() != RUNNER_TYPE) {
      return job.getStatus();
    }

    try {
      // TODO use extId to make API call quicker
      var flinkJob = getFlinkJob(job.getId());
      return FlinkJobStateMapper.map(flinkJob.getState());
    } catch (Exception e) {
      log.error(
          "Unable to retrieve status of a dataflow job with id : {}\ncause: {}",
          job.getExtId(),
          e.getMessage());
    }

    return JobStatus.UNKNOWN;
  }

  private String submitFlinkJob(
      String jobName,
      List<FeatureSetProto.FeatureSet> featureSetProtos,
      SourceProto.Source source,
      StoreProto.Store sink,
      boolean update) {
    try {
      ImportOptions pipelineOptions = getPipelineOptions(jobName, featureSetProtos, sink, update);
      FlinkRunnerResult pipelineResult = runPipeline(pipelineOptions);
      FlinkJob job = waitForJobToRun(pipelineOptions, pipelineResult);
      if (job == null) {
        return null;
      }
      return job.jid;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  private ImportOptions getPipelineOptions(
      String jobName,
      List<FeatureSetProto.FeatureSet> featureSets,
      StoreProto.Store sink,
      boolean update)
      throws IOException {
    String[] args = TypeConversion.convertMapToArgs(defaultOptions);
    ImportOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).as(ImportOptions.class);

    OptionCompressor<List<FeatureSetProto.FeatureSet>> featureSetJsonCompressor =
        new BZip2Compressor<>(new FeatureSetJsonByteConverter());

    pipelineOptions.setFeatureSetJson(featureSetJsonCompressor.compress(featureSets));
    pipelineOptions.setStoreJson(Collections.singletonList(JsonFormat.printer().print(sink)));
    pipelineOptions.setDefaultFeastProject(Project.DEFAULT_NAME);
    // TODO pipelineOptions.setUpdate(update);
    pipelineOptions.setRunner(FlinkRunner.class);
    pipelineOptions.setJobName(jobName);
    pipelineOptions.setFilesToStage(
        detectClassPathResourcesToStage(FlinkRunner.class.getClassLoader()));
    pipelineOptions.setFlinkMaster(config.getMasterUrl());
    // pipelineOptions.setParallelism(Integer value);
    // pipelineOptions.setMaxParallelism(Integer value);
    // pipelineOptions.setCheckpointingInterval(Long interval);
    // pipelineOptions.setCheckpointingMode(String mode);
    // pipelineOptions.setCheckpointTimeoutMillis(Long checkpointTimeoutMillis);
    // pipelineOptions.setMinPauseBetweenCheckpoints(Long minPauseInterval);
    // pipelineOptions.setFailOnCheckpointingErrors(Boolean failOnCheckpointingErrors);
    // pipelineOptions.setNumberOfExecutionRetries(Integer retries);
    // pipelineOptions.setExecutionRetryDelay(Long delay);
    // pipelineOptions.setObjectReuse(Boolean reuse);
    // pipelineOptions.setStateBackendFactory(Class<? extends FlinkStateBackendFactory>
    // stateBackendFactory);
    // pipelineOptions.setEnableMetrics(Boolean enableMetrics);
    // pipelineOptions.setExternalizedCheckpointsEnabled(Boolean externalCheckpoints);
    // pipelineOptions.setRetainExternalizedCheckpointsOnCancellation(Boolean retainOnCancellation);
    // pipelineOptions.setMaxBundleSize(Long size);
    // pipelineOptions.setMaxBundleTimeMills(Long time);
    // pipelineOptions.setShutdownSourcesOnFinalWatermark(Boolean shutdownOnFinalWatermark);
    // pipelineOptions.setLatencyTrackingInterval(Long interval);
    // pipelineOptions.setAutoWatermarkInterval(Long interval);
    // pipelineOptions.setExecutionModeForBatch(String executionMode);
    // pipelineOptions.setSavepointPath(String path);
    // pipelineOptions.setAllowNonRestoredState(Boolean allowNonRestoredState);
    // pipelineOptions.setAutoBalanceWriteFilesShardingEnabled(Boolean
    // autoBalanceWriteFilesShardingEnabled);
    pipelineOptions.setProject(""); // set to default value to satisfy validation

    if (metrics.isEnabled()) {
      pipelineOptions.setMetricsExporterType(metrics.getType());
      if (metrics.getType().equals("statsd")) {
        pipelineOptions.setStatsdHost(metrics.getHost());
        pipelineOptions.setStatsdPort(metrics.getPort());
      }
    }
    return pipelineOptions;
  }

  public FlinkRunnerResult runPipeline(ImportOptions pipelineOptions) throws IOException {
    return (FlinkRunnerResult) ImportJob.runPipeline(pipelineOptions);
  }

  private FlinkJob waitForJobToRun(ImportOptions pipelineOptions, FlinkRunnerResult pipelineResult)
      throws RuntimeException, InterruptedException {
    var job = getFlinkJob(pipelineOptions.getJobName());
    // TODO: add timeout
    while (true) {
      State state = pipelineResult.getState();
      if (state.isTerminal()) {
        throw new RuntimeException(
            String.format("Failed to submit flink job, job state is %s", state.toString()));
      } else if (state.equals(State.RUNNING)) {
        return job;
      }
      Thread.sleep(2000);
    }
  }

  private FlinkJob getFlinkJob(String jobId) {
    FlinkJobList jobList = flinkRestApis.getJobsOverview();
    for (FlinkJob job : jobList.getJobs()) {
      if (jobId.equals(job.getName())) {
        return job;
      }
    }
    log.warn("Unable to find job: {}", jobId);
    return null;
  }

  private String[] createStopArgs(String extId) {
    List<String> commands = new ArrayList<>();
    commands.add("cancel");
    commands.add("-m");
    commands.add(config.getMasterUrl());
    commands.add(extId);
    return commands.toArray(new String[] {});
  }
}