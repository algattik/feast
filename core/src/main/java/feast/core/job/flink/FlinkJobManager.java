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
import feast.ingestion.ImportJob;
import feast.ingestion.options.BZip2Compressor;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.options.OptionCompressor;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.FlinkRunnerResult;
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

    this.defaultOptions =
        Collections.singletonMap("flinkMaster", runnerConfigOptions.get("masterUrl"));
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
    return getJobStatus(job.getId());
  }

  private JobStatus getJobStatus(String jobName) {

    try {
      FlinkJob flinkJob = getFlinkJob(jobName);
      return FlinkJobStateMapper.map(flinkJob.getState());
    } catch (Exception e) {
      log.error(
          "Unable to retrieve status of a dataflow job with id : {}\ncause: {}",
          jobName,
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
      flinkCli.parseParameters(createRunArgs(jobName, featureSetProtos, sink, update));

      FlinkJob job = waitForJobToRun(jobName);
      if (job == null) {
        log.error("No ID returned for job");
        return null;
      }
      return job.jid;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  public FlinkRunnerResult runPipeline(ImportOptions pipelineOptions) throws IOException {
    return (FlinkRunnerResult) ImportJob.runPipeline(pipelineOptions);
  }

  private FlinkJob waitForJobToRun(String jobName) throws RuntimeException, InterruptedException {
    // TODO: add timeout
    while (true) {
      FlinkJob job = getFlinkJob(jobName);
      JobStatus state = getJobStatus(jobName);
      if (state.isTerminal()) {
        throw new RuntimeException(
            String.format("Failed to submit flink job, job state is %s", state.toString()));
      } else if (state.equals(JobStatus.RUNNING)) {
        return job;
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
      }
    }
  }

  private FlinkJob getFlinkJob(String jobName) {
    FlinkJobList jobList = flinkRestApis.getJobsOverview();
    for (FlinkJob job : jobList.getJobs()) {
      if (jobName.equals(job.getName())) {
        return job;
      }
    }
    log.warn("Unable to find job: {}", jobName);
    return null;
  }

  private String[] createRunArgs(
      String jobName,
      List<FeatureSetProto.FeatureSet> featureSets,
      StoreProto.Store sink,
      // TODO update
      boolean update)
      throws IOException {
    List<String> commands = new ArrayList<>();
    commands.add("run");
    commands.add("-d");
    commands.add("-m");
    commands.add(config.getMasterUrl());
    commands.add("/opt/feast/feast-core/BOOT-INF/lib/feast-ingestion-dev.jar");

    OptionCompressor<List<FeatureSetProto.FeatureSet>> featureSetJsonCompressor =
        new BZip2Compressor<>(new FeatureSetJsonByteConverter());

    commands.add(
        option(
            "featureSetJson",
            String.format(
                "\"%s\"",
                Base64.getEncoder()
                    .encodeToString(featureSetJsonCompressor.compress(featureSets)))));
    commands.add(option("storeJson", JsonFormat.printer().print(sink)));
    commands.add(option("defaultFeastProject", Project.DEFAULT_NAME));
    commands.add(option("runner", FlinkRunner.class.getName()));
    commands.add(option("jobName", jobName));
    commands.add(option("project", "dummy"));
    commands.add(
        option("filesToStage", "/opt/feast/feast-core/BOOT-INF/lib/feast-ingestion-dev.jar"));

    // Other available options:
    // parallelism(Integer value);
    // maxParallelism(Integer value);
    // checkpointingInterval(Long interval);
    // checkpointingMode(String mode);
    // checkpointTimeoutMillis(Long checkpointTimeoutMillis);
    // minPauseBetweenCheckpoints(Long minPauseInterval);
    // failOnCheckpointingErrors(Boolean failOnCheckpointingErrors);
    // numberOfExecutionRetries(Integer retries);
    // executionRetryDelay(Long delay);
    // objectReuse(Boolean reuse);
    // stateBackendFactory(Class<? extends FlinkStateBackendFactory> stateBackendFactory);
    // enableMetrics(Boolean enableMetrics);
    // externalizedCheckpointsEnabled(Boolean externalCheckpoints);
    // retainExternalizedCheckpointsOnCancellation(Boolean retainOnCancellation);
    // maxBundleSize(Long size);
    // maxBundleTimeMills(Long time);
    // shutdownSourcesOnFinalWatermark(Boolean shutdownOnFinalWatermark);
    // latencyTrackingInterval(Long interval);
    // autoWatermarkInterval(Long interval);
    // executionModeForBatch(String executionMode);
    // savepointPath(String path);
    // allowNonRestoredState(Boolean allowNonRestoredState);
    // autoBalanceWriteFilesShardingEnabled(Boolean autoBalanceWriteFilesShardingEnabled);

    return commands.toArray(new String[] {});
  }

  private String[] createStopArgs(String extId) {
    List<String> commands = new ArrayList<>();
    commands.add("cancel");
    commands.add("-m");
    commands.add(config.getMasterUrl());
    commands.add(extId);
    return commands.toArray(new String[] {});
  }

  private String option(String key, String value) {
    return String.format("--%s=%s", key, value);
  }
}
