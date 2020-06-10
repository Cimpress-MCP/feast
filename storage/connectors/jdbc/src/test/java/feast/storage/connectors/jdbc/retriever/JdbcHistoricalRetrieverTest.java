package feast.storage.connectors.jdbc.retriever;
import static feast.storage.common.testing.TestUtil.field;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.StoreProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetriever;
import feast.storage.api.writer.FeatureSink;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

@RunWith(MockitoJUnitRunner.class)
public class JdbcHistoricalRetrieverTest {
    @Rule public transient TestPipeline p = TestPipeline.create();
    private HistoricalRetriever sqliteFeatureRetriever;

    private String staging_location = "";
    private String url = "jdbc:postgresql://localhost:5432/postgres";
    private String class_name = "org.postgresql.Driver";
    private String username = "postgres";
    private String pw = System.getenv("postgres_pw");
    private Connection conn;
    private Map<String, String> config = new HashMap<>();
    //TODO: init datasetSource

    ServingAPIProto.DatasetSource mockDatasetSource = mock(ServingAPIProto.DatasetSource.class);


    @Before
    public void setUp() {
        config.put("class_name",class_name);
        config.put("username",username);
        config.put("password",pw);
        config.put("url", url);
        config.put("staging_location", staging_location);

        sqliteFeatureRetriever = JdbcHistoricalRetriever.create(config);
    }
    @Test
    public void shouldRetrieveFromPostgresql() {
        String retrievalId = "1234";
        FeatureSetRequest featureSetRequest =
                FeatureSetRequest.newBuilder()
                        .setSpec(getFeatureSetSpec())
                        .addFeatureReference(
                                ServingAPIProto.FeatureReference.newBuilder().setName("feature_1").setProject("myproject2").build())
                        .addFeatureReference(
                                ServingAPIProto.FeatureReference.newBuilder().setName("feature_2").setProject("myproject2").build())
                        .build();
        List< FeatureSetRequest > featureSetRequests = new ArrayList<>();
        featureSetRequests.add(featureSetRequest);
        System.out.println(mockDatasetSource.getFileSource());
        sqliteFeatureRetriever.getHistoricalFeatures(retrievalId, mockDatasetSource, featureSetRequests);

    }
    private FeatureSetProto.FeatureSetSpec getFeatureSetSpec() {
        return FeatureSetProto.FeatureSetSpec.newBuilder()
                .setProject("myproject2")
                .setName("feature_set")
                .addEntities(EntitySpec.newBuilder().setName("entity_id_primary"))
                .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_1"))
                .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
                .build();
    }




}



