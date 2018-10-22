package com.malt.mongopostgresqlstreamer.connectors.postgres;

import com.malt.mongopostgresqlstreamer.connectors.Connector;
import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import com.malt.mongopostgresqlstreamer.model.FlattenMongoDocument;
import com.malt.mongopostgresqlstreamer.model.TableMapping;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.math.BigDecimal;
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
    public void createTable(
            String mappingName,
            DatabaseMapping mapping
    ) {
        TableMapping tableMapping = getTableMappingOrFail(mappingName, mapping);

        log.info("Importing {} from the beginning...", tableMapping.getSourceCollection());
        log.debug("Preparing initial import for collection {}", tableMapping.getSourceCollection());

        sqlExecutor.dropTable(tableMapping.getDestinationName());
        sqlExecutor.createTable(tableMapping.getDestinationName(), tableMapping.getFieldMappings());

        List<String> relatedTables = getRelatedTables(tableMapping);
        for (String relatedTable : relatedTables) {
            createTable(relatedTable, mapping);
        }
    }

    @Override
    public void addConstraints(
            String mappingName,
            DatabaseMapping mapping) {

        TableMapping tableMapping = getTableMappingOrFail(mappingName, mapping);

        sqlExecutor.setTableAsLogged(tableMapping.getDestinationName());
        sqlExecutor.addPrimaryKey(tableMapping.getDestinationName(), tableMapping.getPrimaryKey());

        for (String index : tableMapping.getIndices()) {
            sqlExecutor.createIndex(index);
        }

        List<String> relatedTables = getRelatedTables(tableMapping);
        for (String relatedTable : relatedTables) {
            addConstraints(relatedTable, mapping);
        }
    }

    @Override
    public void upsert(String mappingName, FlattenMongoDocument document, DatabaseMapping mappings) {
        TableMapping tableMapping = getTableMappingOrFail(mappingName, mappings);
        List<Field> mappedFields = keepOnlyMappedFields(document, tableMapping);

        // Ensure that the primary key is here when we want to insert the `document`.
        // For related document, this primary key may be missing or is null, so we have to generate one.
        String primaryKeyName = tableMapping.getPrimaryKey();
        List<Field> sqlQueryFields = withPrimaryKeyIfNecessary(mappedFields, primaryKeyName);

        removeAllRelatedRecords(mappings, tableMapping, document);

        sqlExecutor.upsert(tableMapping.getDestinationName(), primaryKeyName, sqlQueryFields);

        importDocumentRelations(document, mappings, tableMapping, primaryKeyName, sqlQueryFields);
    }

    @Override
    public void insert(String mappingName, FlattenMongoDocument document, DatabaseMapping mappings) {
        upsert(mappingName, document, mappings);
    }

    @Override
    public void update(String mappingName, FlattenMongoDocument document, DatabaseMapping mappings) {
        upsert(mappingName, document, mappings);
    }

    private void removeByForeignKey(String MappingName, String fieldName, Object value, DatabaseMapping mappings) {
        TableMapping tableMapping = getTableMappingOrFail(MappingName, mappings);
        sqlExecutor.remove(
                tableMapping.getDestinationName(),
                fieldName,
                value
                );
    }

    @Override
    public void remove(String mappingName, FlattenMongoDocument document, DatabaseMapping mappings) {
        TableMapping tableMapping = getTableMappingOrFail(mappingName, mappings);
        Object primaryKeyValue = getPrimaryKeyValue(document, tableMapping);
        sqlExecutor.remove(tableMapping.getDestinationName(), tableMapping.getPrimaryKey(), primaryKeyValue);
    }

    @Override
    public void bulkInsert(
            String mappingName,
            long totalNumberOfDocuments,
            Stream<FlattenMongoDocument> documents,
            DatabaseMapping mappings) {

        bulkInsert(mappingName, totalNumberOfDocuments, documents, mappings, false);
    }

    private int bulkInsert(
            String mappingName,
            long totalNumberOfDocuments,
            Stream<FlattenMongoDocument> documents,
            DatabaseMapping mappings,
            boolean relatedCollection) {

        long startTime = System.currentTimeMillis();

        AtomicInteger counter = new AtomicInteger();
        TableMapping tableMapping = getTableMappingOrFail(mappingName, mappings);

        if (!relatedCollection) {
            log.info("Starting bulk insert of collection {} ({} documents)...", tableMapping.getSourceCollection(), totalNumberOfDocuments);
        } else {
            log.trace("Starting bulk insert of collection {} ({} documents)...", tableMapping.getSourceCollection(), totalNumberOfDocuments);
        }

        String destinationName = tableMapping.getDestinationName();
        documents
                .forEach(document -> {
                    int nbInsertions = importDocument(document, mappings, tableMapping);

                    int tmpCounter = counter.addAndGet(nbInsertions);
                    if (tmpCounter % 1000 == 0) {
                        long endTime = System.currentTimeMillis();
                        double processTimeInSeconds = (endTime - startTime)/1000D;
                        log.info("{} documents imported to {} - speed : {}/s", tmpCounter, destinationName, tmpCounter/processTimeInSeconds);
                    }
                });

        sqlExecutor.finalizeBatchInsert(destinationName);

        if (!relatedCollection) {
            log.info("{} and its related collections was successfully imported ({} documents) !", tableMapping.getSourceCollection(), counter.get());
        } else {
            log.trace("Bulk insert of collection {} done : {} documents inserted", tableMapping.getSourceCollection(), counter.get());
        }

        return counter.get();
    }

    private int importDocument(FlattenMongoDocument document, DatabaseMapping mappings, TableMapping tableMapping) {
        String primaryKeyName = tableMapping.getPrimaryKey();
        String destinationName = tableMapping.getDestinationName();
        List<Field> mappedFields = keepOnlyMappedFields(document, tableMapping);
        List<Field> sqlQueryFields = withPrimaryKeyIfNecessary(mappedFields, primaryKeyName);
        sqlExecutor.batchInsert(destinationName, tableMapping.getFieldMappings(), sqlQueryFields);

        return importDocumentRelations(document, mappings, tableMapping, primaryKeyName, sqlQueryFields) + 1;
    }

    private int importDocumentRelations(
            FlattenMongoDocument document,
            DatabaseMapping mappings,
            TableMapping tableMapping,
            String primaryKeyName,
            List<Field> fields) {

        Field primaryKey = fields.stream()
                .filter(field -> field.getName().equals(primaryKeyName))
                .findFirst()
                .orElse(null);

        return importRelatedCollections(mappings, tableMapping, document, primaryKey);
    }

    private void removeAllRelatedRecords(DatabaseMapping mappings, TableMapping tableMapping, FlattenMongoDocument document) {
        List<String> relatedCollections = getRelatedCollections(tableMapping);
        for (String relatedCollection : relatedCollections) {
            Optional<FieldMapping> optFieldMapping = tableMapping.getBySourceName(relatedCollection);
            if (!optFieldMapping.isPresent()) {
                continue;
            }

            String foreignKey = optFieldMapping.get().getForeignKey();
            if (isBlank(foreignKey)) {
                log.warn("Related table must have a foreign key. None found. {} removal skipped.", relatedCollection);
                continue;
            }

            Object foreignKeyValue = getPrimaryKeyValue(document, tableMapping);

            removeByForeignKey(optFieldMapping.get().getDestinationName(), foreignKey, foreignKeyValue, mappings);
        }
    }

    private int importRelatedCollections(DatabaseMapping mappings, TableMapping tableMapping, FlattenMongoDocument document, Field primaryKey) {
        List<String> relatedCollections = getRelatedCollections(tableMapping);

        AtomicInteger counter = new AtomicInteger();
        for (String relatedCollection : relatedCollections) {
            Optional<FieldMapping> optFieldMapping = tableMapping.getBySourceName(relatedCollection);
            if (!optFieldMapping.isPresent()) {
                continue;
            }

            String foreignKey = optFieldMapping.get().getForeignKey();
            if (isBlank(foreignKey)) {
                log.warn("Related table must have a foreign key. None found. {} import skipped.", relatedCollection);
                continue;
            }

            Object primaryKeyValue = primaryKey.getValue();

            List<FlattenMongoDocument> relatedDocuments = extractFlattenDocumentsFromRelatedCollection(
                    document, relatedCollection, foreignKey, primaryKeyValue, optFieldMapping.get()
            );

            counter.addAndGet(
                    bulkInsert(
                            optFieldMapping.get().getDestinationName(),
                            relatedDocuments.size(),
                            relatedDocuments.stream(),
                            mappings,
                            true
                    )
            );
        }

        return counter.get();
    }

    private Object getPrimaryKeyValue(FlattenMongoDocument document, TableMapping tableMapping) {
        Optional<FieldMapping> optPrimaryKeyMapping = tableMapping.getByDestinationName(tableMapping.getPrimaryKey());
        if (!optPrimaryKeyMapping.isPresent()) {
            log.error("No primary key mapping found for document {}. Generating a random one...", document);
            return new ObjectId().toString();
        }

        Optional<Object> optDocumentPrimaryKey = document.get(optPrimaryKeyMapping.get().getSourceName());
        if (!optDocumentPrimaryKey.isPresent()) {
            log.error("No primary key value found for document {}. Generating a random one...", document);
            return new ObjectId().toString();
        }

        return optDocumentPrimaryKey.map(pk -> pk instanceof ObjectId ? pk.toString() : pk).get();
    }

    private List<Field> withPrimaryKeyIfNecessary(List<Field> currentTableFields, String primaryKey) {
        Field pkField = currentTableFields.stream()
                .filter(field -> field.getName().equals(primaryKey))
                .findFirst()
                .orElse(null);

        if (pkField == null) {
            // Force the ID field to the field mappings in this case.
            currentTableFields.add(new Field(primaryKey, new ObjectId().toString()));
        } else if (pkField.getValue() == null) {
            // Generate an ID if it is missing.
            pkField.setValue(new ObjectId().toString());
        }

        return currentTableFields;
    }

    private List<FlattenMongoDocument> extractFlattenDocumentsFromRelatedCollection(
            FlattenMongoDocument root,
            String relatedCollection,
            String foreignKeyName, Object foreignKeyValue,
            FieldMapping fieldMapping
    ) {
        Optional<Object> optRelatedDocuments = root.get(relatedCollection);
        if (!optRelatedDocuments.isPresent()) {
            log.warn("No field corresponding to the specified related collection ({}) was found.", relatedCollection);
            return Collections.emptyList();
        }

        List<?> relatedDocuments = (List) optRelatedDocuments.get();
        if (relatedDocuments.isEmpty()) {
            return Collections.emptyList();
        }

        Object firstRelatedDocument = relatedDocuments.get(0);
        // Array of documents
        if (firstRelatedDocument instanceof Map) {
            //noinspection unchecked
            return ((List<Map>) optRelatedDocuments.get()).stream()
                    .map(document -> FlattenMongoDocument.fromMap(document)
                            .withField(foreignKeyName, foreignKeyValue))
                    .collect(toList());
        }

        // Array of scalars
        if (isBlank(fieldMapping.getScalarFieldDestinationName())) {
            log.error("'valueName' is mandatory for scalar arrays but was not found in mapping for {}. Import of {} skipped",
                    relatedCollection, relatedCollection);
            return Collections.emptyList();
        }

        return relatedDocuments.stream()
                .map(value -> generateDocumentFromScalar(
                        foreignKeyName,
                        foreignKeyValue,
                        fieldMapping.getScalarFieldDestinationName(),
                        value
                ))
                .collect(toList());

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

    private List<String> getRelatedCollections(TableMapping mapping) {
        return mapping.getFieldMappings().stream()
                .filter(FieldMapping::isAnArray)
                .map(FieldMapping::getSourceName)
                .collect(toList());
    }

    private static List<Field> keepOnlyMappedFields(FlattenMongoDocument document, TableMapping mappings) {
        Map<String, Object> values = document.getValues();
        return mappings.getFieldMappings().stream()
                .filter(fieldMapping -> !fieldMapping.isAnArray())
                .map(fieldMapping -> toField(fieldMapping, values))
                .collect(toList());
    }

    private static Field toField(FieldMapping fieldMapping, Map<String, Object> document) {
        return new Field(
                fieldMapping.getDestinationName(),
                transform(document.get(fieldMapping.getSourceName()), fieldMapping.getTrueType())
        );
    }

    private static Object transform(Object value, String type) {
        if (type.equalsIgnoreCase("_PRESENCE")) {
            return value != null;
        }
        if (type.startsWith("DOUBLE PRECISION") && value instanceof String) {
            return new BigDecimal((String)value);
        }
        return value;
    }

    private List<String> getRelatedTables(TableMapping tableMapping) {
        return tableMapping.getArrayFieldMappings().stream()
                .map(FieldMapping::getDestinationName)
                .collect(toList());
    }

    private TableMapping getTableMappingOrFail(String mappingName, DatabaseMapping mappings) {
        return mappings.get(mappingName)
                .orElseThrow(() -> new RuntimeException("No defined mapping for mappingName " + mappingName + "."));
    }
}
