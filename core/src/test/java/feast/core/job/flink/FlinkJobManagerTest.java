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

public class FlinkJobManagerTest {

  /*
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  @Mock
  private CliFrontend flinkCli;
  @Mock
  private FlinkRestApi flinkRestApi;
  private FlinkJobConfig config;
  private ImportJobDefaults defaults;
  private FlinkJobManager flinkJobManager;
  private Path workspace;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    config = new FlinkJobConfig("localhost:8081", "/etc/flink/conf");
    workspace = Paths.get(tempFolder.newFolder().toString());
    defaults =
        ImportJobDefaults.builder().
            runner("FlinkRunner").importJobOptions("{\"key\":\"value\"}")
            .executable("ingestion.jar")
            .workspace(workspace.toString()).build();

    flinkJobManager = new FlinkJobManager(flinkCli, config, flinkRestApi, defaults);
  }

  @Test
  public void shouldPassCorrectArgumentForSubmittingJob() throws IOException {
    FlinkJobList response = new FlinkJobList();
    response.setJobs(Collections.singletonList(new FlinkJob("1234", "job1", "RUNNING")));
    when(flinkRestApi.getJobsOverview()).thenReturn(response);

    String jobName = "importjob";

    flinkJobManager.startJob(jobName, Paths.get("/tmp/foobar"));
    String[] expected =
        new String[]{
            "run",
            "-d",
            "-m",
            config.getMasterUrl(),
            defaults.getExecutable(),
            "--jobName=" + jobName,
            "--runner=FlinkRunner",
            "--workspace=/tmp/foobar",
            "--key=value"
        };

    ArgumentCaptor<String[]> argumentCaptor = ArgumentCaptor.forClass(String[].class);
    verify(flinkCli).parseParameters(argumentCaptor.capture());

    String[] actual = argumentCaptor.getValue();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnFlinkJobId() {
    FlinkJobList response = new FlinkJobList();
    String flinkJobId = "1234";
    String jobName = "importjob";
    response.setJobs(Collections.singletonList(new FlinkJob(flinkJobId, jobName, "RUNNING")));
    when(flinkRestApi.getJobsOverview()).thenReturn(response);

    String jobId = flinkJobManager.startJob(jobName, workspace);

    assertThat(jobId, equalTo(flinkJobId));
  }

  @Test
  public void shouldPassCorrectArgumentForStoppingJob() {
    String jobId = "1234";

    flinkJobManager.abortJob(jobId);

    String[] expected = new String[]{
        "cancel",
        "-m",
        config.getMasterUrl(),
        jobId
    };

    ArgumentCaptor<String[]> argumentCaptor = ArgumentCaptor.forClass(String[].class);
    verify(flinkCli).parseParameters(argumentCaptor.capture());

    String[] actual = argumentCaptor.getValue();
    assertThat(actual, equalTo(expected));
  }
        */
}
