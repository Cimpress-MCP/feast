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

import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;

import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetStatus;
import feast.proto.core.SourceProto;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class SnowflakeTestUtils {
  static SourceProto.Source defaultSource =
      SnowflakeTestUtils.createSource("kafka:9092,localhost:9094", "feast-features");

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

  public static void applyFeatureSet(
      CoreSimpleAPIClient secureApiClient,
      String projectName,
      String featureSetName,
      String entityId,
      String featureName) {
    List<Pair<String, ValueProto.ValueType.Enum>> entities = new ArrayList<>();
    entities.add(Pair.of(entityId, ValueProto.ValueType.Enum.INT64));
    List<Pair<String, ValueProto.ValueType.Enum>> features = new ArrayList<>();
    features.add(Pair.of(featureName, ValueProto.ValueType.Enum.INT64));
    FeatureSetProto.FeatureSet expectedFeatureSet =
        SnowflakeTestUtils.createFeatureSet(
            SnowflakeTestUtils.getDefaultSource(), projectName, featureSetName, entities, features);
    secureApiClient.simpleApplyFeatureSet(expectedFeatureSet);
//    waitAtMost(2, TimeUnit.MINUTES)
//        .until(
//            () -> {
//              return secureApiClient.simpleGetFeatureSet(projectName, featureSetName).getMeta();
//            },
//            hasProperty("status", equalTo(FeatureSetStatus.STATUS_READY)));
//    FeatureSetProto.FeatureSet actualFeatureSet =
//        secureApiClient.simpleGetFeatureSet(projectName, featureSetName);
//    assertEquals(
//        expectedFeatureSet.getSpec().getProject(), actualFeatureSet.getSpec().getProject());
//    assertEquals(expectedFeatureSet.getSpec().getName(), actualFeatureSet.getSpec().getName());
//    assertEquals(expectedFeatureSet.getSpec().getSource(), actualFeatureSet.getSpec().getSource());
//    assertEquals(FeatureSetStatus.STATUS_READY, actualFeatureSet.getMeta().getStatus());
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

  public static ServingServiceGrpc.ServingServiceBlockingStub getServingServiceStub(
      int feastServingPort) {
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", feastServingPort).usePlaintext().build();

    return ServingServiceGrpc.newBlockingStub(secureChannel);
  }

  public static CoreSimpleAPIClient getApiClientForCore(int feastCorePort) {
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", feastCorePort).usePlaintext().build();

    CoreServiceGrpc.CoreServiceBlockingStub secureCoreService =
        CoreServiceGrpc.newBlockingStub(secureChannel);
    return new CoreSimpleAPIClient(secureCoreService);
  }
}
