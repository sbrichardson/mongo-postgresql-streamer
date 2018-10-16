package com.malt.mongopostgresqlstreamer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.malt.mongopostgresqlstreamer.config.StreamerTestConfig;
import com.mongodb.MongoClient;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.jdbc.JdbcTestUtils.countRowsInTable;

@SpringBootTest(classes = StreamerTestConfig.class)
@ContextConfiguration(initializers = {StreamerIntegrationTest.Initializer.class})
@ActiveProfiles("it")
@ExtendWith(SpringExtension.class)
class StreamerIntegrationTest {
    private static final String DATA_FILE_NAME = "data-it.json";
    private static final int MONGO_PORT = 27017;
    private static final PostgreSQLContainer postgreSQLContainer =
            (PostgreSQLContainer) new PostgreSQLContainer("postgres:10.4")
                    .withDatabaseName("streamer")
                    .withUsername("user")
                    .withPassword("password")
                    .withStartupTimeout(Duration.ofSeconds(600));

    private static final GenericContainer mongoContainer =
            new GenericContainer("mongo:3.2.16")
                    .withCommand("--replSet \"rs0\"")
                    .withExposedPorts(MONGO_PORT);

    @Autowired
    private InitialImporter initialImporter;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void setUp() throws IOException, InterruptedException {
        postgreSQLContainer.start();
        mongoContainer.start();
        mongoContainer.execInContainer("/usr/bin/mongo", "--eval", "rs.initiate()");
    }

    @AfterAll
    static void cleanUp() {
        postgreSQLContainer.stop();
        mongoContainer.stop();
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            EnvironmentTestUtils.addEnvironment("testcontainers", configurableApplicationContext.getEnvironment(),
                    "spring.datasource.url=" + postgreSQLContainer.getJdbcUrl(),
                    "spring.datasource.username=" + postgreSQLContainer.getUsername(),
                    "spring.datasource.password=" + postgreSQLContainer.getPassword(),
                    "mongo.uri=mongodb://localhost:" + mongoContainer.getMappedPort(MONGO_PORT)
            );
        }
    }

    @Test
    void should_import_data_from_mongo_to_pgsql() throws IOException {
        loadData();

        initialImporter.start();

        assertThat(countRowsInTable(jdbcTemplate, "superheros")).isEqualTo(20);
        assertThat(countRowsInTable(jdbcTemplate, "superhero_characters")).isEqualTo(33);
        assertThat(countRowsInTable(jdbcTemplate, "superheros_marvel")).isEqualTo(10);
    }

    private void loadData() throws IOException {
        InputStream is = new ClassPathResource(DATA_FILE_NAME).getInputStream();
        JsonArray data = new JsonParser().parse(new InputStreamReader(is)).getAsJsonArray();
        List<Document> documents = new ArrayList<>(data.size());
        for (JsonElement jsonElement: data) {
            documents.add(Document.parse(jsonElement.toString()));
        }

        mongoClient.getDatabase("my_db")
                .getCollection("superheros")
                .insertMany(documents);
    }
}