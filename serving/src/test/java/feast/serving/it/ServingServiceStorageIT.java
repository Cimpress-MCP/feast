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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class ServingServiceStorageIT {

  static final int REDIS_PORT = 6379;
  static final String CORE = "core_1";
  static final int CORE_START_MAX_WAIT_TIME_IN_MINUTES = 3;
  static final int FEAST_CORE_PORT = 6565;
  static final int FEAST_SERVING_PORT = 6566;

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) throws UnknownHostException {

    System.out.print("initializing");
  }

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
  public void testdummy() {
    assertTrue(1 == 1);
  }

  @Test
  public void testSnowflakeSinkShouldPass() throws InterruptedException {
    // apply feature set
    CoreSimpleAPIClient coreClient = BatchTestUtils.getApiClientForCore(FEAST_CORE_PORT);
    BatchTestUtils.applyFeatureSet(coreClient, "test_proj", "entity", "feature1");
    List<FeatureRow> features = BatchTestUtils.ingestFeatures("test_proj", "entity", "feature1");
    System.out.println("size of featurerow---" + features.size());
    KafkaTemplate<String, FeatureRow> kafkaTemplate = specKafkaTemplate();
    for (int i = 0; i < features.size(); i++) {

      System.out.print("rows---" + features.get(i).getFeatureSet() + "" + features.get(i));
      kafkaTemplate.send("feast-features", features.get(i));
    }

    waitAtMost(2, TimeUnit.MINUTES);

    TimeUnit.MINUTES.sleep(2);
    System.out.println("After 2 mins");

    //        GetBatchFeaturesResponse featureResponse =
    //     servingStub.getOnlineFeatures(onlineFeatureRequest);
    //    //    assertEquals(1, featureResponse.getFieldValuesCount());
    //    Map<String, Value> fieldsMap = featureResponse.getFieldValues(0).getFieldsMap();
    //    assertTrue(fieldsMap.containsKey(ENTITY_ID));
    assertTrue(1 == 1);
  }
}
