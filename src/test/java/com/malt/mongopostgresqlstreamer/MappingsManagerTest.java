package com.malt.mongopostgresqlstreamer;

import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.Mappings;
import com.malt.mongopostgresqlstreamer.model.TableMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

class MappingsManagerTest {
    private static final String FILE_NAME = "mapping.json";

    private String filePath;
    private MappingsManager mappingsManager;

    @BeforeEach
    void setUp() {
        mappingsManager = new MappingsManager();
        filePath = Objects.requireNonNull(this.getClass().getClassLoader().getResource(FILE_NAME)).getPath();
    }


    @Test
    void should_parse_valid_mapping_file() throws FileNotFoundException {
        Mappings result = mappingsManager.read(filePath);

        assertThat(result).isNotNull();

        List<DatabaseMapping> databaseMappings = result.getDatabaseMappings();
        assertThat(databaseMappings).isNotNull()
                .hasSize(1)
                .extracting("name")
                .containsExactly("my_mongo_database");

        List<TableMapping> tableMappings = databaseMappings.get(0).getTableMappings();
        assertThat(tableMappings).isNotNull()
                .hasSize(3);

        TableMapping table1 = tableMappings.get(0);
        // When we don't specify any source or destination, we use the mapping name
        assertThat(table1.getMappingName()).isEqualTo("my_mongo_collection");
        assertThat(table1.getSourceCollection()).isEqualTo("my_mongo_collection");
        assertThat(table1.getDestinationName()).isEqualTo("my_mongo_collection");
        assertThat(table1.getFieldMappings()).isNotNull()
                .extracting("sourceName", "destinationName", "type")
                .containsExactly(
                        tuple("_creationdate", "_creationdate", "TIMESTAMP"),
                        tuple("_id", "id", "TEXT")
                );
        assertThat(table1.getFilters()).isEmpty();
        assertThat(table1.getIndices()).isNotEmpty();

        TableMapping table2 = tableMappings.get(1);
        assertThat(table2.getMappingName()).isEqualTo("custom_mapping_name");
        assertThat(table2.getSourceCollection()).isEqualTo("my_mongo_collection");
        assertThat(table2.getDestinationName()).isEqualTo("my_destination_table");
        assertThat(table2.getFieldMappings()).isNotNull()
                .extracting("sourceName", "destinationName", "type")
                .containsExactly(
                        tuple("_creationdate", "_creationdate", "TIMESTAMP"),
                        tuple("_id", "id", "TEXT"),
                        tuple("description", "description", "TEXT")
                );
        assertThat(table2.getFilters()).isEmpty();

        TableMapping table3 = tableMappings.get(1);
        assertThat(table3.getMappingName()).isEqualTo("custom_mapping_name");
        assertThat(table3.getSourceCollection()).isEqualTo("my_mongo_collection");
        assertThat(table3.getDestinationName()).isEqualTo("my_destination_table");
        assertThat(table3.getFieldMappings()).isNotNull()
                .extracting("sourceName", "destinationName", "type")
                .containsExactly(
                        tuple("_creationdate", "_creationdate", "TIMESTAMP"),
                        tuple("_id", "id", "TEXT"),
                        tuple("description", "description", "TEXT")
                );
        assertThat(table3.getFilters()).isEmpty();

    }
}
