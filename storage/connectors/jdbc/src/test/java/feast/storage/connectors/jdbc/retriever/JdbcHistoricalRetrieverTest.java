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
package feast.storage.connectors.jdbc.retriever;

import com.google.protobuf.Duration;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetriever;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JdbcHistoricalRetrieverTest {
  @Rule public transient TestPipeline p = TestPipeline.create();
  private HistoricalRetriever postgresqlFeatureRetriever;

  //    Give postgresql writing permission to your staging_location
  //    run: sudo chmod 777 <staging_location>
  private String staging_location = System.getenv("staging_location");
  private String url = "jdbc:postgresql://localhost:5432/postgres";
  private String class_name = "org.postgresql.Driver";
  private String username = "postgres";
  private String pw = System.getenv("postgres_pw");
  private Map<String, String> postgressqlConfig = new HashMap<>();

  @Test
  public void dummyTest() {
    Assert.assertEquals(1, 1);
  }
//  @Before
//  public void setUp() {
//    postgressqlConfig.put("class_name", class_name);
//    postgressqlConfig.put("username", username);
//    postgressqlConfig.put("password", pw);
//    postgressqlConfig.put("url", url);
//    postgressqlConfig.put("staging_location", staging_location);
//
//    postgresqlFeatureRetriever = JdbcHistoricalRetriever.create(postgressqlConfig);
//  }
//
//  @Test
//  public void shouldRetrieveFromPostgresql() {
//    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be the
//    // entity_id
//    //      file_uri is under
//    // src/test/java/feast/storage/connectors/jdbc/retriever/myproject2_entity_rows.csv
//    String file_uris = System.getenv("file_uri");
//    ServingAPIProto.DatasetSource.FileSource fileSource =
//        ServingAPIProto.DatasetSource.FileSource.newBuilder()
//            .setDataFormatValue(2)
//            .addFileUris(file_uris)
//            .build();
//
//    ServingAPIProto.DatasetSource datasetSource =
//        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();
//
//    String retrievalId = "1234";
//    FeatureSetRequest featureSetRequest =
//        FeatureSetRequest.newBuilder()
//            .setSpec(getFeatureSetSpec())
//            .addFeatureReference(
//                ServingAPIProto.FeatureReference.newBuilder()
//                    .setName("feature_1")
//                    .setProject("myproject2")
//                    .setFeatureSet("feature_set")
//                    .build())
//            .build();
//    List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
//    featureSetRequests.add(featureSetRequest);
//
//    System.out.println(datasetSource.getFileSource());
//    postgresqlFeatureRetriever.getHistoricalFeatures(
//        retrievalId, datasetSource, featureSetRequests);
//  }
//
//  private FeatureSetProto.FeatureSetSpec getFeatureSetSpec() {
//    return FeatureSetProto.FeatureSetSpec.newBuilder()
//        .setProject("myproject2")
//        .setName("feature_set")
//        .addEntities(EntitySpec.newBuilder().setName("entity_id_primary"))
//        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_1"))
//        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
//        .build();
//  }
}
