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

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetStatus;
import feast.proto.core.SourceProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.apache.commons.lang3.tuple.Pair;

public class SnowflakeTestUtils {
  public static final String SFurl = System.getenv("SFurl");
  static final String PROJECT_NAME = "myproject4";
  static final String FEATURE_SET = "feature_set";
  static final String ENTITY_NAME = "ENTITY_ID_PRIMARY";
  static final String TEST_FEATURE_1 = "FEATURE_1";

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
      String featureName,
      Integer maxAgeSec) {
    List<Pair<String, ValueProto.ValueType.Enum>> entities = new ArrayList<>();
    entities.add(Pair.of(entityId, ValueProto.ValueType.Enum.INT64));
    List<Pair<String, ValueProto.ValueType.Enum>> features = new ArrayList<>();
    features.add(Pair.of(featureName, ValueProto.ValueType.Enum.INT64));
    // create a feature set
    FeatureSetProto.FeatureSet expectedFeatureSet =
        SnowflakeTestUtils.createFeatureSet(
            SnowflakeTestUtils.getDefaultSource(), projectName, featureSetName, entities, features);
    if (maxAgeSec != null) {
      expectedFeatureSet =
          SnowflakeTestUtils.createFeatureSetWithMaxAge(
              SnowflakeTestUtils.getDefaultSource(),
              projectName,
              featureSetName,
              entities,
              features,
              maxAgeSec);
    }

    secureApiClient.simpleApplyFeatureSet(expectedFeatureSet);
    waitAtMost(2, TimeUnit.MINUTES)
        .until(
            () -> {
              return secureApiClient.simpleGetFeatureSet(projectName, featureSetName).getMeta();
            },
            hasProperty("status", equalTo(FeatureSetStatus.STATUS_READY)));
    FeatureSetProto.FeatureSet actualFeatureSet =
        secureApiClient.simpleGetFeatureSet(projectName, featureSetName);
    assertEquals(
        expectedFeatureSet.getSpec().getProject(), actualFeatureSet.getSpec().getProject());
    assertEquals(expectedFeatureSet.getSpec().getName(), actualFeatureSet.getSpec().getName());
    assertEquals(expectedFeatureSet.getSpec().getSource(), actualFeatureSet.getSpec().getSource());
    assertEquals(FeatureSetStatus.STATUS_READY, actualFeatureSet.getMeta().getStatus());
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

  public static FeatureSetProto.FeatureSet createFeatureSetWithMaxAge(
      SourceProto.Source source,
      String projectName,
      String name,
      List<Pair<String, ValueProto.ValueType.Enum>> entities,
      List<Pair<String, ValueProto.ValueType.Enum>> features,
      Integer maxAgeSec) {
    return FeatureSetProto.FeatureSet.newBuilder()
        .setSpec(
            FeatureSetProto.FeatureSetSpec.newBuilder()
                .setMaxAge(Duration.newBuilder().setSeconds(maxAgeSec))
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

  public static List<FeatureRow> ingestFeatures(String projectName, String featureSetName)
      throws ParseException {
    String featureSet = projectName + "/" + featureSetName;
    String ingestion_id = getIngestionID(featureSetName);
    List<FeatureRow> featureRows =
        ImmutableList.of(
            buildFeatureRow(ingestion_id, "2019-12-31T16:00:00.00Z", featureSet, 1, 400),
            buildFeatureRow(ingestion_id, "2019-12-31T18:00:00.00Z", featureSet, 1, 410),
            buildFeatureRow(ingestion_id, "2019-12-31T20:00:00.00Z", featureSet, 2, 100),
            buildFeatureRow(ingestion_id, "2020-01-01T08:00:00Z", featureSet, 2, 220),
            buildFeatureRow(ingestion_id, "2020-01-01T10:00:00Z", featureSet, 3, 300));

    return featureRows;
  }

  private static FeatureRow buildFeatureRow(
      String ingestion_id,
      String event_timestamp,
      String featureSet,
      int entity_id__primary,
      int feature_1)
      throws ParseException {
    return FeatureRow.newBuilder()
        .setIngestionId(ingestion_id)
        .setEventTimestamp(Timestamps.parse(event_timestamp))
        .setFeatureSet(featureSet)
        .addFields(field(ENTITY_NAME, entity_id__primary, Enum.INT64))
        .addFields(field(TEST_FEATURE_1, feature_1, Enum.INT64))
        .build();
  }

  private static String getIngestionID(String featureSetName) {
    return featureSetName + new Date().getTime();
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

  /**
   * create GetBatchFeaturesRequest with single feature
   *
   * @param entitySourceUri
   * @param feature
   * @param featureSet
   * @param project
   * @return
   */
  public static ServingAPIProto.GetBatchFeaturesRequest createGetBatchFeaturesRequest(
      String entitySourceUri, String feature, String featureSet, String project) {
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .addFileUris(entitySourceUri)
            .setDataFormat(ServingAPIProto.DataFormat.DATA_FORMAT_CSV)
            .build();
    ServingAPIProto.FeatureReference featureReference =
        ServingAPIProto.FeatureReference.newBuilder()
            .setName(feature)
            .setFeatureSet(featureSet)
            .setProject(project)
            .build();
    ServingAPIProto.GetBatchFeaturesRequest getBatchFeaturesRequest =
        ServingAPIProto.GetBatchFeaturesRequest.newBuilder()
            .addFeatures(0, featureReference)
            .setDatasetSource(
                ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build())
            .setComputeStatistics(false)
            .build();
    return getBatchFeaturesRequest;
  }

  /**
   * get the result feature set in s3 as a list of lines
   *
   * @param fileUri
   * @return
   * @throws IOException
   */
  public static List<String> readFromS3(String fileUri) throws IOException {
    AmazonS3 s3client =
        AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProvider()).build();
    String bucket = new AmazonS3URI(fileUri).getBucket();
    String key = new AmazonS3URI(fileUri).getKey();
    S3Object fileObj = s3client.getObject(new GetObjectRequest(bucket, key));
    Scanner fileIn = new Scanner(new GZIPInputStream(fileObj.getObjectContent()));
    List<String> resultLines = new ArrayList<>();
    if (null != fileIn) {
      while (fileIn.hasNext()) {
        resultLines.add(fileIn.nextLine());
      }
    }
    return resultLines;
  }
}
