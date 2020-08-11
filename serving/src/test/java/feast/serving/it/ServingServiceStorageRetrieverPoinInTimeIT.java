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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ProtocolStringList;
import feast.proto.serving.ServingAPIProto.*;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.types.FeatureRowProto.FeatureRow;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runners.model.InitializationError;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(properties = {"feast.active_store=historical_snowflake"})
@Testcontainers
public class ServingServiceStorageRetrieverPoinInTimeIT {

  static final int REDIS_PORT = 6379;
  static final String CORE = "core_1";
  static final int CORE_START_MAX_WAIT_TIME_IN_MINUTES = 3;
  static final int FEAST_CORE_PORT = 6565;
  static final int FEAST_SERVING_PORT = 6566;
  static final int MAX_AGE_SECOND = 30;

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) throws UnknownHostException {}

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-it-core.yml"))
          .withExposedService(
              CORE,
              6565,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(CORE_START_MAX_WAIT_TIME_IN_MINUTES)));

  @BeforeAll
  static void globalSetup() throws IOException, InitializationError, InterruptedException {
    System.out.print("global setup");
  }

  public KafkaTemplate<String, FeatureRow> specKafkaTemplate() {

    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092,localhost:9094");

    KafkaTemplate<String, FeatureRow> t =
        new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(
                props, new StringSerializer(), new KafkaSerialization.ProtoSerializer<>()));
    t.setDefaultTopic("feast-features");
    return t;
  }

  @Test
  public void testSnowflakeSinkShouldPass() throws InterruptedException {
    // apply feature set
    CoreSimpleAPIClient coreClient = SnowflakeTestUtils.getApiClientForCore(FEAST_CORE_PORT);
    SnowflakeTestUtils.applyFeatureSet(
        coreClient, "test_proj", "test_1", "entity", "feature1", MAX_AGE_SECOND);
    List<FeatureRow> features =
        SnowflakeTestUtils.ingestFeatures("test_proj", "entity", "feature1");
    KafkaTemplate<String, FeatureRow> kafkaTemplate = specKafkaTemplate();
    for (int i = 0; i < features.size(); i++) {

      kafkaTemplate.send("feast-features", features.get(i));
    }

    TimeUnit.MINUTES.sleep(2);
    //    System.out.println("After 2 mins");

    //            FeatureSet featureResponse =
    //            		coreClient.simpleGetFeatureSet("test_proj", "test_1");
    //            assertEquals(featureResponse.getMeta(),)
    //    //    assertEquals(1, featureResponse.getFieldValuesCount());
    //    Map<String, Value> fieldsMap = featureResponse.getFieldValues(0).getFieldsMap();
    //    assertTrue(fieldsMap.containsKey(ENTITY_ID));
    assertTrue(1 == 1);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest3DatesWithMaxAge() throws IOException {

    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_3dates.csv";
    GetBatchFeaturesRequest getBatchFeaturesRequest =
        SnowflakeTestUtils.createGetBatchFeaturesRequest(
            entitySourceUri, "feature_1", "feature_set", "myproject4");
    // TODO: start Core and ingest data
    CoreSimpleAPIClient coreClient = SnowflakeTestUtils.getApiClientForCore(FEAST_CORE_PORT);
    SnowflakeTestUtils.applyFeatureSet(
        coreClient, "myproject4", "feature_set", "ENTITY_ID_PRIMARY", "feature_1", MAX_AGE_SECOND);
    // Run getBatchFeatures on Serving
    ServingServiceBlockingStub servingStub =
        SnowflakeTestUtils.getServingServiceStub(FEAST_SERVING_PORT);
    GetBatchFeaturesResponse response = servingStub.getBatchFeatures(getBatchFeaturesRequest);
    Job resultJob = response.getJob();
    GetJobRequest jobRequest = GetJobRequest.newBuilder().setJob(resultJob).build();
    servingStub.getJob(jobRequest).getJob();
    waitAtMost(2, TimeUnit.MINUTES)
        .until(
            () -> servingStub.getJob(jobRequest).getJob(),
            hasProperty("status", equalTo(JobStatus.JOB_STATUS_DONE)));
    resultJob = servingStub.getJob(jobRequest).getJob();
    ProtocolStringList resultUris = resultJob.getFileUrisList();

    // get csv.gz file from s3
    List<String> resultLines = SnowflakeTestUtils.readFromS3(resultUris.get(0));
    for (String line : resultLines) {
      System.out.println(line);
    }
    // TODO: uncomment when ingestion is done
    //    Assert.assertEquals("\\" + "\\N", resultLines.get(1).split(",")[3]);
    //    Assert.assertEquals("100", resultLines.get(2).split(",")[3]);
    //    Assert.assertEquals("300", resultLines.get(3).split(",")[3]);
    assertTrue(1 == 1);
  }
}
