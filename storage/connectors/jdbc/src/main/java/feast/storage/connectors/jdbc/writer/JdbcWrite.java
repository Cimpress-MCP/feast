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
package feast.storage.connectors.jdbc.writer;

import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;

/**
 * A {@link PTransform} that writes {@link FeatureRowProto FeatureRows} to the specified BigQuery
 * dataset, and returns a {@link WriteResult} containing the unsuccessful writes. Since Bigquery
 * does not output successful writes, we cannot emit those, and so no success metrics will be
 * captured if this sink is used.
 */
public class JdbcWrite extends PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcWrite.class);
  private final JdbcTemplater jdbcTemplater;
  private final StoreProto.Store.JdbcConfig config;

  public JdbcWrite(JdbcConfig config, JdbcTemplater jdbcTemplater) {

    this.config = config;
    this.jdbcTemplater = jdbcTemplater;
  }

  public StoreProto.Store.JdbcConfig getConfig() {
    return config;
  }

  public JdbcTemplater getJdbcTemplater() {
    return jdbcTemplater;
  }

  @Override
  public WriteResult expand(PCollection<FeatureRowProto.FeatureRow> input) {
    String jobName = input.getPipeline().getOptions().getJobName();

    int batchSize = this.config.getBatchSize() > 0 ? config.getBatchSize() : 1;

    PCollection<FeatureRow> feature = input;

    feature.apply(
        "WriteFeaturestoTable",
        ParDo.of(new InsertFeatureSetToTable(this.getJdbcTemplater(), this.getConfig(), jobName)));

    PCollection<FeatureRowProto.FeatureRow> successfulInserts =
        input.apply(
            "dummy",
            ParDo.of(
                new DoFn<FeatureRowProto.FeatureRow, FeatureRowProto.FeatureRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {}
                }));

    PCollection<FailedElement> failedElements =
        input.apply(
            "dummyFailed",
            ParDo.of(
                new DoFn<FeatureRowProto.FeatureRow, FailedElement>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {}
                }));

    return WriteResult.in(input.getPipeline(), successfulInserts, failedElements);
  }
}
