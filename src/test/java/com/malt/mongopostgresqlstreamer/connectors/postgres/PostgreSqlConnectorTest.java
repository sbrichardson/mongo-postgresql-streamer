package com.malt.mongopostgresqlstreamer.connectors.postgres;

import com.malt.mongopostgresqlstreamer.model.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
        TableMapping tableMapping = givenTableUsersMapping();
        DatabaseMapping dbMapping = givenDatabaseMapping("users", tableMapping);

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

    @Test
    void it_should_bulk_insert_table_with_related_table_entry() {
        DatabaseMapping dbMapping = givenDatabaseMapping("teams", givenTableTeamMapping(), givenTableTeamMembersMapping());
        FlattenMongoDocument flattenedDocument = FlattenMongoDocument.fromDocument(givenTeamDocument());

        connector.bulkInsert("teams", 1, Stream.of(flattenedDocument), dbMapping);

        verifyBulkInsert();
    }

    @Test
    void it_should_bulk_insert_table_with_related_table_entry_with_iterable_entry_set_to_null() {
        DatabaseMapping dbMapping = givenDatabaseMapping("teams", givenTableTeamMapping(), givenTableTeamMembersMapping());
        FlattenMongoDocument flattenedDocument = FlattenMongoDocument.fromDocument(givenTeamDocumentWithoutMembers());

        connector.bulkInsert("teams", 1, Stream.of(flattenedDocument), dbMapping);

        verifyBulkInsert();
    }

    @Test
    @SuppressWarnings("unchecked")
    void it_should_bulk_insert_table_and_related_collection() {
        DatabaseMapping dbMapping = givenDatabaseMapping("teams", givenTableTeamMapping(), givenTableTeamMembersMapping());
        FlattenMongoDocument flattenedDocument = FlattenMongoDocument.fromDocument(givenTeamDocument());

        connector.bulkInsert("teams", 1, Stream.of(flattenedDocument), dbMapping);

        ArgumentCaptor<List<FieldMapping>> argFieldMappings = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<Field>> argFields = ArgumentCaptor.forClass(List.class);
        verify(sqlExecutor, times(2)).batchInsert(eq("team_members"), argFieldMappings.capture(), argFields.capture());

        List<List<FieldMapping>> fieldMappings = argFieldMappings.getAllValues();
        List<List<Field>> fields = argFields.getAllValues();

        verifyBulkInsertRelatedCollection(fieldMappings.get(0), fields.get(0));
        verifyBulkInsertRelatedCollection(fieldMappings.get(1), fields.get(1));
    }

    private static void verifyBulkInsertRelatedCollection(List<FieldMapping> fieldMappings, List<Field> fields) {
        assertThat(fieldMappings).hasSize(4)
                .extracting(
                        FieldMapping::getSourceName,
                        FieldMapping::getDestinationName,
                        FieldMapping::getType
                )
                .contains(
                        tuple("_creationdate", "_creationdate", "TIMESTAMP"),
                        tuple("team_id", "team_id", "TEXT"),
                        tuple("name", "name", "TEXT"),
                        tuple("", "id", "VARCHAR")
                );

        assertThat(fields).hasSize(4)
                .extracting(Field::getName)
                .contains("_creationdate", "team_id", "name", "id");

        Field pkField = fields.stream().filter(field -> field.getName().equals("id")).findFirst().get();
        assertThat(pkField.getValue())
                .as("The primary key field must never be null")
                .isNotNull();
    }

    @SuppressWarnings("unchecked")
    private void verifyBulkInsert() {
        ArgumentCaptor<List<FieldMapping>> argFieldMappings = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<Field>> argFields = ArgumentCaptor.forClass(List.class);
        verify(sqlExecutor).batchInsert(eq("teams"), argFieldMappings.capture(), argFields.capture());

        List<FieldMapping> fieldMappings = argFieldMappings.getValue();
        assertThat(fieldMappings).hasSize(4)
                .extracting(
                        FieldMapping::getSourceName,
                        FieldMapping::getDestinationName,
                        FieldMapping::getType
                )
                .contains(
                        tuple("_creationdate", "_creationdate", "TIMESTAMP"),
                        tuple("_id", "id", "TEXT"),
                        tuple("name", "name", "TEXT"),
                        tuple("members", "team_members", "_ARRAY")
                );

        List<Field> fields = argFields.getValue();
        assertThat(fields).hasSize(3)
                .extracting(Field::getName)
                .contains("_creationdate", "id", "name");
    }

    private static Map<String, Object> givenUser() {
        Map<String, Object> user = new HashMap<>();
        user.put("_id", UUID.randomUUID().toString());
        user.put("name", "John Doe");
        return user;
    }

    private static DatabaseMapping givenDatabaseMapping(String name, TableMapping tableMapping, TableMapping... others) {
        DatabaseMapping databaseMapping = new DatabaseMapping();
        databaseMapping.setName(name);

        List<TableMapping> tableMappings = new ArrayList<>();
        tableMappings.add(tableMapping);
        Collections.addAll(tableMappings, others);
        databaseMapping.setTableMappings(tableMappings);

        return databaseMapping;
    }

    private static TableMapping givenTableUsersMapping() {
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

    private static TableMapping givenTableTeamMapping() {
        TableMapping mapping = new TableMapping();
        mapping.setPrimaryKey("id");
        mapping.setSourceCollection("teams");
        mapping.setDestinationName("teams");
        mapping.setMappingName("teams");
        mapping.setIndices(singletonList("INDEX idx_teams__creationdate ON teams (_creationdate)"));
        mapping.setFieldMappings(asList(
                givenFieldMapping("_creationdate", "_creationdate", "TIMESTAMP"),
                givenFieldMapping("_id", "id", "TEXT"),
                givenFieldMapping("name", "name", "TEXT"),
                givenFieldMapping("members", "team_members", "_ARRAY", "team_id")
        ));

        return mapping;
    }

    private static TableMapping givenTableTeamMembersMapping() {
        TableMapping mapping = new TableMapping();
        mapping.setPrimaryKey("id");
        mapping.setSourceCollection("team_members");
        mapping.setDestinationName("team_members");
        mapping.setMappingName("team_members");
        mapping.setIndices(asList(
                "INDEX idx_team_members__creationdate ON team_members (_creationdate)",
                "INDEX idx_team_members_team_id ON team_members (team_id)"
        ));

        mapping.setFieldMappings(asList(
                givenFieldMapping("_creationdate", "_creationdate", "TIMESTAMP"),
                givenFieldMapping("team_id", "team_id", "TEXT"),
                givenFieldMapping("name", "name", "TEXT"),
                givenFieldMapping("", "id", "VARCHAR")
        ));

        return mapping;
    }

    private static FieldMapping givenFieldMapping(String sourceName, String destinationName, String type) {
        FieldMapping fieldMapping = new FieldMapping();
        fieldMapping.setSourceName(sourceName);
        fieldMapping.setDestinationName(destinationName);
        fieldMapping.setType(type);
        fieldMapping.setScalarFieldDestinationName(type);
        fieldMapping.setIndexed(false);
        fieldMapping.setForeignKey(null);
        return fieldMapping;
    }

    private static FieldMapping givenFieldMapping(String sourceName, String destinationName, String type, String foreignKey) {
        FieldMapping fieldMapping = new FieldMapping();
        fieldMapping.setSourceName(sourceName);
        fieldMapping.setDestinationName(destinationName);
        fieldMapping.setType(type);
        fieldMapping.setForeignKey(foreignKey);
        fieldMapping.setIndexed(false);
        fieldMapping.setScalarFieldDestinationName(type);
        return fieldMapping;
    }

    private static Document givenTeamDocument() {
        Document teamDocument = new Document();
        teamDocument.put("id", new ObjectId().toHexString());
        teamDocument.put("name", "The Avengers");
        teamDocument.put("members", asList(
                givenTeamMemberDocument("Hulk"),
                givenTeamMemberDocument("Iron Man")
        ));

        return teamDocument;
    }

    private static Document givenTeamDocumentWithoutMembers() {
        Document teamDocument = new Document();
        teamDocument.put("id", new ObjectId().toHexString());
        teamDocument.put("name", "The Avengers");
        return teamDocument;
    }

    private static Document givenTeamMemberDocument(String name) {
        Document document = new Document();
        document.put("id", null);
        document.put("name", name);
        return document;
    }
}
