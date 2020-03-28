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
package feast.core.job.spark;

import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.beam.sdk.PipelineResult;

@Getter
@AllArgsConstructor
public class SparkJob {

  private String jobId;
  private PipelineResult pipelineResult;

  public void abort() throws IOException {
    if (!pipelineResult.getState().isTerminal()) {
      pipelineResult.cancel();
    }
  }
}
