package com.malt.mongopostgresqlstreamer.connectors.postgres;

import com.malt.mongopostgresqlstreamer.connectors.postgres.PostgreSqlConnector;
import com.malt.mongopostgresqlstreamer.connectors.postgres.SqlExecutor;
import com.malt.mongopostgresqlstreamer.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PostgreSqlConnectorTest {

    private SqlExecutor sqlExecutor;
    private PostgreSqlConnector connector;

    @BeforeEach
    void setUp() {
        sqlExecutor = mock(SqlExecutor.class);
        connector = new PostgreSqlConnector(sqlExecutor);
    }

    @Test
    @SuppressWarnings("unchecked")
    void it_should_upsert_document_and_include_missing_field() {
        Map<String, Object> document = givenUser();
        FlattenMongoDocument flattenedDocument = FlattenMongoDocument.fromMap(document);
        TableMapping tableMapping = givenTableMapping();
        DatabaseMapping dbMapping = givenDatabaseMapping(tableMapping);

        connector.upsert(tableMapping.getMappingName(), flattenedDocument, dbMapping);

        ArgumentCaptor<List<Field>> argFields = ArgumentCaptor.forClass(List.class);
        verify(sqlExecutor).upsert(eq("users"), eq("id"), argFields.capture());

        List<Field> fields = argFields.getValue();
        assertThat(fields).hasSize(3)
                .extracting(Field::getName, Field::getValue)
                .contains(
                        tuple("id", document.get("_id")),
                        tuple("name", document.get("name")),
                        tuple("birthday", null)
                );
    }

    private static Map<String, Object> givenUser() {
        Map<String, Object> user = new HashMap<>();
        user.put("_id", UUID.randomUUID().toString());
        user.put("name", "John Doe");
        return user;
    }

    private static DatabaseMapping givenDatabaseMapping(TableMapping tableMapping) {
        DatabaseMapping databaseMapping = new DatabaseMapping();
        databaseMapping.setName("users");
        databaseMapping.setTableMappings(singletonList(tableMapping));
        return databaseMapping;
    }

    private static TableMapping givenTableMapping() {
        TableMapping mapping = new TableMapping();
        mapping.setPrimaryKey("id");
        mapping.setSourceCollection("users");
        mapping.setDestinationName("users");
        mapping.setMappingName("users-mappings");
        mapping.setFieldMappings(asList(
                givenFieldMapping("_id", "id", "TEXT"),
                givenFieldMapping("name", "name", "TEXT"),
                givenFieldMapping("birthday", "birthday", "TIMESTAMP")
        ));

        return mapping;
    }

    private static FieldMapping givenFieldMapping(String sourceName, String destinationName, String type) {
        FieldMapping fieldMapping = new FieldMapping();
        fieldMapping.setSourceName(sourceName);
        fieldMapping.setDestinationName(destinationName);
        fieldMapping.setType(type);
        return fieldMapping;
    }
}
