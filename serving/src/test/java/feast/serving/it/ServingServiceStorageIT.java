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

import static org.junit.jupiter.api.Assertions.assertTrue;

import feast.proto.serving.ServingAPIProto.*;
import feast.proto.serving.ServingAPIProto.DatasetSource.FileSource;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runners.model.InitializationError;
import org.springframework.boot.test.context.SpringBootTest;
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
    //
    //    System.out.print("initializing");
    //    registry.add("feast.stores[0].name", () -> "online");
    //    registry.add("feast.stores[0].type", () -> "REDIS");
    // Redis needs to accessible by both core and serving, hence using host address
    //    registry.add(
    //        "feast.stores[0].config.host",
    //        () -> {
    //          try {
    //            return InetAddress.getLocalHost().getHostAddress();
    //          } catch (UnknownHostException e) {
    //            // TODO Auto-generated catch block
    //            e.printStackTrace();
    //            return "";
    //          }
    //        });
    //    registry.add("feast.stores[0].config.port", () -> REDIS_PORT);
    //    registry.add("feast.stores[0].subscriptions[0].name", () -> "*");
    //    registry.add("feast.stores[0].subscriptions[0].project", () -> "*");
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

  @Test
  public void testdummy() {
    assertTrue(1 == 1);
  }

  @Test
  public void shouldRetrieveFromSnowflake() {

    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_3dates.csv";
    FileSource fileSource =
        FileSource.newBuilder()
            .addFileUris(entitySourceUri)
            .setDataFormat(DataFormat.DATA_FORMAT_CSV)
            .build();
    FeatureReference pr4_fs_fr1 =
        FeatureReference.newBuilder()
            .setName("feature_1")
            .setFeatureSet("feature_set")
            .setProject("myproject4")
            .build();
    GetBatchFeaturesRequest getBatchFeaturesRequest =
        GetBatchFeaturesRequest.newBuilder()
            .addFeatures(0, pr4_fs_fr1)
            .setDatasetSource(DatasetSource.newBuilder().setFileSource(fileSource).build())
            .setComputeStatistics(false)
            .build();
    // TODO: Core
    CoreSimpleAPIClient coreClient = SnowflakeTestUtils.getApiClientForCore(FEAST_CORE_PORT);
    SnowflakeTestUtils.applyFeatureSet(
        coreClient, "myproject4", "feature_set", "ENTITY_ID_PRIMARY", "feature_1");
    // TODO: Serving
    ServingServiceBlockingStub servingStub =
        SnowflakeTestUtils.getServingServiceStub(FEAST_SERVING_PORT);

    GetBatchFeaturesResponse response = servingStub.getBatchFeatures(getBatchFeaturesRequest);
    System.out.println(response);
    assertTrue(1 == 1);
  }
}