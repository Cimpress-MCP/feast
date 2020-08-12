/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import static feast.storage.common.testing.TestUtil.field;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcSnowflakeFeatureSinkMockTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private JdbcFeatureSink snowflakeFeatureSinkObj;
  private StoreProto.Store.JdbcConfig jdbcConfig;
  private String userName = "fakeUsername";
  private String password = "fakePassword";
  private String database = "DEMO_DB";
  private String schema = "PUBLIC";
  private String warehouse = "COMPUTE_WH";
  private String snowflakeUrl = "jdbc:snowflake://nx46274.us-east-2.aws.snowflakecomputing.com";
  private String className = "net.snowflake.client.jdbc.SnowflakeDriver";
  private String tableName = "feast_features";
  private Map<FeatureSetReference, FeatureSetProto.FeatureSetSpec> specMap;
  @Mock JdbcTemplate jdbcTemplate;
  private String testDriverClassName = "org.sqlite.JDBC";
  private String testDbUrl = "jdbc:sqlite:memory:myDb";
  private String testDbUsername = "sa";
  private String tesDbPassword = "sa";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    // setup featuresets
    FeatureSetProto.FeatureSetSpec spec1 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("feature_set_1")
            .setProject("snowflake_proj")
            .build();

    FeatureSetReference ref1 = FeatureSetReference.of(spec1.getProject(), spec1.getName(), 1);

    FeatureSetProto.FeatureSetSpec spec2 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("feature_set_2")
            .setProject("snowflake_proj")
            .build();
    FeatureSetReference ref2 = FeatureSetReference.of(spec2.getProject(), spec2.getName(), 1);

    specMap = ImmutableMap.of(ref1, spec1, ref2, spec2);
    this.jdbcConfig =
        StoreProto.Store.JdbcConfig.newBuilder()
            .setUrl(this.snowflakeUrl)
            .setClassName(this.className)
            .setUsername(this.userName)
            .setPassword(this.password)
            .setDatabase(this.database)
            .setSchema(this.schema)
            .setWarehouse(this.warehouse)
            .setTableName(this.tableName)
            .setBatchSize(1) // This must be set to 1 for DirectRunner
            .build();
    this.snowflakeFeatureSinkObj = (JdbcFeatureSink) JdbcFeatureSink.fromConfig(this.jdbcConfig);
  }

  @Test
  public void shouldStartPrepareWrite() {
    // Mocking the jdbcTemplate
    JdbcFeatureSink fakeSnowflakeFeatureSinkObj = spy(snowflakeFeatureSinkObj);
    doNothing().when(jdbcTemplate).execute(any(String.class));
    Mockito.doReturn(jdbcTemplate).when(fakeSnowflakeFeatureSinkObj).createJdbcTemplate();
    PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs =
        p.apply("CreateSchema", Create.of(specMap));
    PCollection<FeatureSetReference> actualSchemas =
        fakeSnowflakeFeatureSinkObj.prepareWrite(featureSetSpecs);
    String actualName = actualSchemas.getName();
    p.run();
    Assert.assertEquals("createSchema/ParMultiDo(Anonymous).output", actualName);
  }

  @Test
  public void shouldWriteToSnowflake() throws Exception {
    JdbcIO.DataSourceConfiguration testConfig =
        JdbcIO.DataSourceConfiguration.create(this.testDriverClassName, this.testDbUrl)
            .withUsername(this.testDbUsername)
            .withPassword(this.tesDbPassword);
    String event_timestamp = "2019-12-31T16:00:00.00Z";
    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_1")
                .setEventTimestamp(Timestamps.parse(event_timestamp))
                .addFields(field("entity", 1, Enum.INT64))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_2")
                .addFields(field("entity", 2, Enum.INT64))
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_2")
                .setIngestionId("table-4")
                .addFields(field("entity_id_primary", 4, Enum.INT32))
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
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
                .addFields(field("feature_2", 4, Enum.INT64))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_3")
                .addFields(field("entity", 2, Enum.INT64))
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .build());

    // Mock jdbcTemplater and jdbcWrite for testing on Sqlite
    JdbcTemplater jdbcTemplater = this.snowflakeFeatureSinkObj.getJdbcTemplater();
    JdbcTemplater testJdbcTemplater = spy(jdbcTemplater);
    JdbcWrite jdbcWrite = new JdbcWrite(this.jdbcConfig, testJdbcTemplater);
    JdbcWrite testJdbcWrite = spy(jdbcWrite);
    when(testJdbcWrite.create_dsconfig(this.jdbcConfig)).thenReturn(testConfig);
    String testInsertionSql =
        "INSERT INTO feast_features (event_timestamp,created_timestamp,project,featureset,feature,ingestion_id,job_id) select ?,?,?,?,json(?),?,?;";
    when(testJdbcTemplater.getFeatureRowInsertSql(this.tableName)).thenReturn(testInsertionSql);

    // Create feast_features table
    String createSqlTableCreationQuery = testJdbcTemplater.getTableCreationSql(this.jdbcConfig);
    JdbcTemplate jdbcTemplate = createTestJdbcTemplate();
    jdbcTemplate.execute(createSqlTableCreationQuery);
    p.apply(Create.of(featureRows)).apply(testJdbcWrite);
    p.run();
    List<Map<String, Object>> resultsRows =
        jdbcTemplate.queryForList("SELECT * FROM feast_features WHERE featureset='feature_set_1'");
    String expectedProject = "snowflake_proj";
    Assert.assertTrue(resultsRows.size() > 0);
    Assert.assertEquals(expectedProject, resultsRows.get(0).get("project"));
  }

  private JdbcTemplate createTestJdbcTemplate() {
    HikariConfig hkConfig = new HikariConfig();
    hkConfig.setMaximumPoolSize(1);
    hkConfig.setDriverClassName(this.testDriverClassName);
    hkConfig.setJdbcUrl(this.testDbUrl);
    hkConfig.setUsername(this.testDbUsername);
    hkConfig.setPassword(this.tesDbPassword);
    final HikariDataSource ds = new HikariDataSource(hkConfig);
    return new JdbcTemplate(ds);
  }
}
