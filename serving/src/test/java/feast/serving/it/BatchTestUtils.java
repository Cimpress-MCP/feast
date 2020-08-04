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

import static feast.storage.common.testing.TestUtil.field;
import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetStatus;
import feast.proto.core.SourceProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class BatchTestUtils {

  static SourceProto.Source defaultSource =
      createSource("kafka:9092,localhost:9094", "feast-features");

  public static SourceProto.Source getDefaultSource() {
    return defaultSource;
  }

  public static SourceProto.Source createSource(String server, String topic) {
    return SourceProto.Source.newBuilder()
        .setType(SourceProto.SourceType.KAFKA)
        .setKafkaSourceConfig(
            SourceProto.KafkaSourceConfig.newBuilder()
                .setBootstrapServers(server)
                .setTopic(topic)
                .build())
        .build();
  }

  public static FeatureSetProto.FeatureSet createFeatureSet(
      SourceProto.Source source,
      String projectName,
      String name,
      List<Pair<String, ValueProto.ValueType.Enum>> entities,
      List<Pair<String, ValueProto.ValueType.Enum>> features) {
    return FeatureSetProto.FeatureSet.newBuilder()
        .setSpec(
            FeatureSetProto.FeatureSetSpec.newBuilder()
                .setSource(source)
                .setName(name)
                .setProject(projectName)
                .addAllEntities(
                    entities.stream()
                        .map(
                            pair ->
                                FeatureSetProto.EntitySpec.newBuilder()
                                    .setName(pair.getLeft())
                                    .setValueType(pair.getRight())
                                    .build())
                        .collect(Collectors.toList()))
                .addAllFeatures(
                    features.stream()
                        .map(
                            pair ->
                                FeatureSetProto.FeatureSpec.newBuilder()
                                    .setName(pair.getLeft())
                                    .setValueType(pair.getRight())
                                    .build())
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  public static void applyFeatureSet(
      CoreSimpleAPIClient apiClient, String projectName, String entityId, String featureName) {
    List<Pair<String, ValueProto.ValueType.Enum>> entities = new ArrayList<>();
    entities.add(Pair.of(entityId, ValueProto.ValueType.Enum.INT64));
    List<Pair<String, ValueProto.ValueType.Enum>> features = new ArrayList<>();
    features.add(Pair.of(featureName, ValueProto.ValueType.Enum.INT64));
    String featureSetName = "test_1";
    FeatureSetProto.FeatureSet expectedFeatureSet =
        BatchTestUtils.createFeatureSet(
            BatchTestUtils.getDefaultSource(), projectName, featureSetName, entities, features);
    apiClient.simpleApplyFeatureSet(expectedFeatureSet);

    waitAtMost(2, TimeUnit.MINUTES)
        .until(
            () -> {
              return apiClient.simpleGetFeatureSet(projectName, featureSetName).getMeta();
            },
            hasProperty("status", equalTo(FeatureSetStatus.STATUS_READY)));
    FeatureSetProto.FeatureSet actualFeatureSet =
        apiClient.simpleGetFeatureSet(projectName, featureSetName);
    assertEquals(
        expectedFeatureSet.getSpec().getProject(), actualFeatureSet.getSpec().getProject());
    assertEquals(expectedFeatureSet.getSpec().getName(), actualFeatureSet.getSpec().getName());
    assertEquals(expectedFeatureSet.getSpec().getSource(), actualFeatureSet.getSpec().getSource());
    assertEquals(FeatureSetStatus.STATUS_READY, actualFeatureSet.getMeta().getStatus());
  }

  public static List<FeatureRow> ingestFeatures(
      String projectName, String entityId, String featureName) {
    // TODO Auto-generated method stub
    String featureSetName = "test_1";
    String featureSet = projectName + "/" + featureSetName;
    String ingestion_id = getIngestionID(featureSetName);
    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setIngestionId(ingestion_id)
                .setFeatureSet(featureSet)
                .addFields(field(entityId, 1, Enum.INT64))
                .addFields(field(featureName, 1, Enum.INT64))
                .build(),
            FeatureRow.newBuilder()
                .setIngestionId(ingestion_id)
                .setFeatureSet(featureSet)
                .addFields(field(entityId, 2, Enum.INT64))
                .addFields(field(featureName, 2, Enum.INT64))
                //		                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .build());

    return featureRows;
  }

  private static String getIngestionID(String featureSetName) {
    return featureSetName + new Date().getTime();
  }

  public static CoreSimpleAPIClient getApiClientForCore(int feastCorePort) {
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", feastCorePort).usePlaintext().build();
    CoreServiceGrpc.CoreServiceBlockingStub secureCoreService =
        CoreServiceGrpc.newBlockingStub(secureChannel);
    return new CoreSimpleAPIClient(secureCoreService);
  }
}
