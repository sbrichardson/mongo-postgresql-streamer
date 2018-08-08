package com.malt.mongopostgresqlstreamer.connectors.postgres;

import com.malt.mongopostgresqlstreamer.connectors.Connector;
import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import com.malt.mongopostgresqlstreamer.model.FlattenMongoDocument;
import com.malt.mongopostgresqlstreamer.model.TableMapping;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
@Service
public class PostgreSqlConnector implements Connector {
    private final SqlExecutor sqlExecutor;

    @Inject
    public PostgreSqlConnector(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public void prepareInitialImport(
            String sourceCollection,
            DatabaseMapping mapping
    ) {
        log.debug("Preparing initial import for collection {}", sourceCollection);

        TableMapping tableMapping = getTableMappingOrFail(sourceCollection, mapping);

        sqlExecutor.dropTable(tableMapping.getDestinationName());
        sqlExecutor.createTable(tableMapping.getDestinationName(), tableMapping.getFieldMappings());
        sqlExecutor.addPrimaryKey(tableMapping.getDestinationName(), tableMapping.getPrimaryKey());

        for (String index : tableMapping.getIndices()) {
            sqlExecutor.createIndex(index);
        }

        List<String> relatedTables = getRelatedTables(tableMapping);
        for (String relatedTable : relatedTables) {
            prepareInitialImport(relatedTable, mapping);
        }
    }

    @Override
    public void upsert(String collection, FlattenMongoDocument document, DatabaseMapping mappings) {
        TableMapping tableMapping = getTableMappingOrFail(collection, mappings);
        List<Field> mappedFields = keepOnlyMappedFields(document, tableMapping);
        List<Field> currentTableFields = getCurrentTableFields(mappedFields);
        sqlExecutor.upsert(
                tableMapping.getDestinationName(),
                tableMapping.getPrimaryKey(),
                withPrimaryKeyIfNecessary(currentTableFields, tableMapping.getPrimaryKey())
        );

        importRelatedCollections(mappings, tableMapping, document);
    }

    @Override
    public void insert(String collection, FlattenMongoDocument document, DatabaseMapping mappings) {
        upsert(collection, document, mappings);
    }

    @Override
    public void update(String collection, FlattenMongoDocument document, DatabaseMapping mappings) {
        upsert(collection, document, mappings);
    }

    @Override
    public void remove(String collection, FlattenMongoDocument document, DatabaseMapping mappings) {
        TableMapping tableMapping = getTableMappingOrFail(collection, mappings);

        sqlExecutor.remove(
                tableMapping.getDestinationName(),
                tableMapping.getPrimaryKey(), getPrimaryKeyValue(document, tableMapping)
        );
    }

    @Override
    public void bulkInsert(String collection, Stream<FlattenMongoDocument> documents, DatabaseMapping mappings) {
        log.debug("Starting bulk insert of collection {}...", collection);

        AtomicInteger counter = new AtomicInteger();
        documents.forEach(document -> {
            upsert(collection, document, mappings);
            counter.incrementAndGet();
        });

        log.debug("Bulk insert of collection {} done : {} documents inserted", collection, counter.get());
    }

    private void importRelatedCollections(DatabaseMapping mappings, TableMapping tableMapping, FlattenMongoDocument document) {
        List<String> relatedCollections = getRelatedCollections(document);
        for (String relatedCollection : relatedCollections) {
            Optional<FieldMapping> optFieldMapping = tableMapping.getBySourceName(relatedCollection);
            if (!optFieldMapping.isPresent()) {
                continue;
            }

            String foreignKey = optFieldMapping.get().getForeignKey();
            if (isBlank(foreignKey)) {
                log.error("Related table must have a foreign key. None found. {} import skipped.", relatedCollection);
                continue;
            }

            bulkInsert(
                    optFieldMapping.get().getDestinationName(),
                    extractFlattenDocumentsFromRelatedCollection(
                            document, relatedCollection, foreignKey, getPrimaryKeyValue(document, tableMapping), optFieldMapping.get()
                    ),
                    mappings
            );
        }
    }

    private Object getPrimaryKeyValue(FlattenMongoDocument document, TableMapping tableMapping) {
        Optional<FieldMapping> optPrimaryKeyMapping = tableMapping.getByDestinationName(tableMapping.getPrimaryKey());
        if (!optPrimaryKeyMapping.isPresent()) {
            log.error("No primary key mapping found for document {}. Generating a random one...", document);
            return UUID.randomUUID().toString();
        }

        Optional<Object> optDocumentPrimaryKey = document.get(optPrimaryKeyMapping.get().getSourceName());
        if (!optDocumentPrimaryKey.isPresent()) {
            log.error("No primary key value found for document {}. Generating a random one...", document);
            return UUID.randomUUID().toString();
        }

        return optDocumentPrimaryKey.get();
    }

    private List<Field> withPrimaryKeyIfNecessary(List<Field> currentTableFields, String primaryKey) {
        boolean hasPrimaryKeyDefined = currentTableFields.stream()
                .anyMatch(field -> field.getName().equals(primaryKey));
        if (hasPrimaryKeyDefined) {
            return currentTableFields;
        }

        currentTableFields.add(new Field(primaryKey, UUID.randomUUID().toString()));
        return currentTableFields;
    }

    private Stream<FlattenMongoDocument> extractFlattenDocumentsFromRelatedCollection(
            FlattenMongoDocument root,
            String relatedCollection,
            String foreignKeyName, Object foreignKeyValue,
            FieldMapping fieldMapping
    ) {
        Optional<Object> optRelatedDocuments = root.get(relatedCollection);
        if (!optRelatedDocuments.isPresent()) {
            log.warn("No field corresponding to the specified related collection ({}) was found.", relatedCollection);
            return Stream.empty();
        }

        List relatedDocuments = (List) optRelatedDocuments.get();
        if (relatedDocuments.isEmpty()) {
            return Stream.empty();
        }

        Object firstRelatedDocument = relatedDocuments.get(0);
        // Array of documents
        if (firstRelatedDocument instanceof Map) {
            return ((List<Map>) optRelatedDocuments.get()).stream()
                    .map(document -> FlattenMongoDocument.fromMap(document)
                            .withField(foreignKeyName, foreignKeyValue));
        }

        // Array of scalars
        if (isBlank(fieldMapping.getScalarFieldDestinationName())) {
            log.error("'valueName' is mandatory for scalar arrays but was not found in mapping for {}. Import of {} skipped",
                    relatedCollection, relatedCollection);
            return Stream.empty();
        }

        return relatedDocuments.stream()
                .map(value -> generateDocumentFromScalar(
                        foreignKeyName,
                        foreignKeyValue,
                        fieldMapping.getScalarFieldDestinationName(),
                        value
                ));

    }

    private FlattenMongoDocument generateDocumentFromScalar(
            String foreignKeyName, Object foreignKeyValue,
            String scalarFieldDestinationName, Object value
    ) {
        Map<String, Object> documentMap = new HashMap<>();
        documentMap.put(foreignKeyName, foreignKeyValue);
        documentMap.put(scalarFieldDestinationName, value);
        return FlattenMongoDocument.fromMap(documentMap);
    }

    private List<String> getRelatedCollections(FlattenMongoDocument document) {
        return document.getValues().entrySet().stream()
                .map(e -> new Field(e.getKey(), e.getValue()))
                .filter(Field::isList)
                .map(Field::getName)
                .collect(toList());
    }

    private List<Field> getCurrentTableFields(List<Field> mappedFields) {
        return mappedFields.stream()
                .filter(field -> !field.isList())
                .collect(toList());
    }

    private List<Field> keepOnlyMappedFields(FlattenMongoDocument document, TableMapping mappings) {
        return document.getValues().entrySet().stream()
                .filter(e -> mappings.isMapped(e.getKey()))
                .map(e -> new Field(
                        mappings.getBySourceName(e.getKey()).get().getDestinationName(),
                        e.getValue()
                ))
                .collect(toList());
    }

    private List<String> getRelatedTables(TableMapping tableMapping) {
        return tableMapping.getArrayFieldMappings().stream()
                .map(FieldMapping::getDestinationName)
                .collect(toList());
    }

    private TableMapping getTableMappingOrFail(String collection, DatabaseMapping mappings) {
        return mappings.get(collection)
                .orElseThrow(() -> new RuntimeException("No mapping defined for collection " + collection + "."));
    }
}
