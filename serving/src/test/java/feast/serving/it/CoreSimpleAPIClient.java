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
package feast.serving.it;

import feast.proto.core.*;
import java.util.List;

public class CoreSimpleAPIClient {
  private CoreServiceGrpc.CoreServiceBlockingStub stub;
  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 9092;
  private static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOST + ":" + KAFKA_PORT;
  private static final short KAFKA_REPLICATION_FACTOR = 1;
  private static final String KAFKA_TOPIC = "topic";
  private static final String KAFKA_SPECS_TOPIC = "topic_specs";
  private static final String KAFKA_SPECS_ACK_TOPIC = "topic_specs_ack";

  private static final long KAFKA_PUBLISH_TIMEOUT_SEC = 10;
  private static final long KAFKA_POLL_TIMEOUT_SEC = 10;

  public CoreSimpleAPIClient(CoreServiceGrpc.CoreServiceBlockingStub stub) {
    this.stub = stub;
  }

  // TODO:
  public void ingestFeatureSet(
      String id,
      IngestionJobProto.IngestionJobStatus status,
      List<FeatureSetProto.FeatureSet> featureSet) {
    IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
            .setSource(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                    .setTopic(KAFKA_SPECS_TOPIC)
                    .build())
            .build();

    //    specsStreamingUpdateConfig.writeTo();
    IngestionJobProto.IngestionJob.newBuilder()
        .setId("123")
        .setExternalId("123")
        .setStatus(status)
        .addAllFeatureSets(featureSet)
        .build();
  }

  public void simpleApplyFeatureSet(FeatureSetProto.FeatureSet featureSet) {
    stub.applyFeatureSet(
        CoreServiceProto.ApplyFeatureSetRequest.newBuilder().setFeatureSet(featureSet).build());
  }

  public FeatureSetProto.FeatureSet simpleGetFeatureSet(String projectName, String name) {
    return stub.getFeatureSet(
            CoreServiceProto.GetFeatureSetRequest.newBuilder()
                .setName(name)
                .setProject(projectName)
                .build())
        .getFeatureSet();
  }
}
