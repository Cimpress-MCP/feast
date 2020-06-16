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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetrievalResult;
import feast.storage.api.retriever.HistoricalRetriever;
import feast.storage.api.writer.FeatureSink;
import feast.storage.connectors.jdbc.writer.JdbcFeatureSink;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static feast.storage.common.testing.TestUtil.field;

public class SnowflakeHistoricalRetrieverTest {
    @Rule
    public transient TestPipeline p = TestPipeline.create();
    private FeatureSink snowflakeFeatureSinkObj;
    private HistoricalRetriever snowflakeFeatureRetriever;
    //  Snowflake account
    private String staging_location = System.getenv("staging_location");
    private Map<String, String> snowflakeConfig = new HashMap<>();
    private String SFUrl = "jdbc:snowflake://ry42518.us-east-2.aws.snowflakecomputing.com";
    private String SFClassName = "net.snowflake.client.jdbc.SnowflakeDriver";
    private String SFusername = "CHIZHANG";
    private String SFpw = "123456Pw";
    private Connection conn;

    @Before
    public void setUp() {

        snowflakeConfig.put("class_name", SFClassName);
        snowflakeConfig.put("username", SFusername);
        snowflakeConfig.put("password", SFpw);
        snowflakeConfig.put("url", SFUrl);
        snowflakeConfig.put("staging_location", staging_location);

        snowflakeFeatureRetriever = JdbcHistoricalRetriever.create(snowflakeConfig);
        FeatureSetProto.FeatureSetSpec spec1 =
                FeatureSetProto.FeatureSetSpec.newBuilder()
                        .setName("feature_set_1")
                        .setProject("snowflake_proj")
                        .addEntities(
                                FeatureSetProto.EntitySpec.newBuilder()
                                        .setName("entity")
                                        .setValueType(ValueProto.ValueType.Enum.INT64)
                                        .build())
                        .addFeatures(
                                FeatureSetProto.FeatureSpec.newBuilder()
                                        .setName("feature")
                                        .setValueType(ValueProto.ValueType.Enum.STRING)
                                        .build())
                        .build();
    }


    @Test
    public void shouldWriteToSnowflake() throws SQLException {

        List<FeatureRowProto.FeatureRow> featureRows =
                ImmutableList.of(
                        FeatureRowProto.FeatureRow.newBuilder()
                                .setFeatureSet("snowflake_proj/feature_set_1")
                                .addFields(field("entity", 1, ValueProto.ValueType.Enum.INT64))
                                .addFields(field("feature", "one", ValueProto.ValueType.Enum.STRING))
                                .build(),
                        FeatureRowProto.FeatureRow.newBuilder()
                                .setFeatureSet("snowflake_proj/feature_set_1")
                                .addFields(field("entity", 2, ValueProto.ValueType.Enum.INT64))
                                .addFields(field("feature", "two", ValueProto.ValueType.Enum.STRING))
                                .build(),
                        FeatureRowProto.FeatureRow.newBuilder()
                                .setFeatureSet("snowflake_proj/feature_set_1")
                                .addFields(field("entity", 3, ValueProto.ValueType.Enum.INT64))
                                .addFields(field("feature", "two", ValueProto.ValueType.Enum.STRING))
                                .build(),
                        FeatureRowProto.FeatureRow.newBuilder()
                                .setFeatureSet("snowflake_proj/feature_set_2")
                                .addFields(field("entity_id_primary", 4, ValueProto.ValueType.Enum.INT32))
                                .addFields(field("entity_id_secondary", "asjdh", ValueProto.ValueType.Enum.STRING))
                                .addFields(
                                        FieldProto.Field.newBuilder()
                                                .setName("feature_1")
                                                .setValue(
                                                        ValueProto.Value.newBuilder()
                                                                .setStringListVal(
                                                                        ValueProto.StringList.newBuilder()
                                                                                .addVal("abc")
                                                                                .addVal("def")
                                                                                .build())
                                                                .build())
                                                .build())
                                .addFields(field("feature_2", 4, ValueProto.ValueType.Enum.INT64))
                                .build());

        p.apply(Create.of(featureRows)).apply(this.snowflakeFeatureSinkObj.writer());
        p.run();
        DatabaseMetaData dbm = conn.getMetaData();
        String expectedTable = "SNOWFLAKE_PROJ_FEATURE_SET_1";
        Assert.assertEquals(true, dbm.getTables(null, null, expectedTable, null).next());
    }


    @Test
    public void shouldRetrieveFromSnowflake() {
        //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
        // entity_id
        //      file_uri is under
        // src/test/java/feast/storage/connectors/jdbc/retriever/snowflake_proj_entity_rows.csv
        String file_uris = System.getenv("snowflake_file_uri");
        ServingAPIProto.DatasetSource.FileSource fileSource =
                ServingAPIProto.DatasetSource.FileSource.newBuilder()
                        .setDataFormatValue(2)
                        .addFileUris(file_uris)
                        .build();

        ServingAPIProto.DatasetSource datasetSource =
                ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();

        String retrievalId = "1234";
        FeatureSetRequest featureSetRequest =
                FeatureSetRequest.newBuilder()
                        .setSpec(getFeatureSetSpec())
                        .addFeatureReference(
                                ServingAPIProto.FeatureReference.newBuilder()
                                        .setName("FEATURE")
                                        .setProject("SNOWFLAKE_PROJ")
                                        .setFeatureSet("FEATURE_SET_1")
                                        .build())
                        .build();
        List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
        featureSetRequests.add(featureSetRequest);

        HistoricalRetrievalResult postgresHisRetrievalResult =
                snowflakeFeatureRetriever.getHistoricalFeatures(
                        retrievalId, datasetSource, featureSetRequests);
        List<String> files = postgresHisRetrievalResult.getFileUris();
        File testFile = new File(files.get(0));
        // Check if file exist in staging location
        Assert.assertTrue(testFile.exists() && !testFile.isDirectory());
        Assert.assertTrue(files.get(0).indexOf(staging_location) != -1);
    }

    private FeatureSetProto.FeatureSetSpec getFeatureSetSpec() {
        return FeatureSetProto.FeatureSetSpec.newBuilder()
                .setProject("SNOWFLAKE_PROJ")
                .setName("FEATURE_SET_1")
                .addEntities(FeatureSetProto.EntitySpec.newBuilder().setName("ENTITY"))
                .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("FEATURE"))
                .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
                .build();
    }

}
