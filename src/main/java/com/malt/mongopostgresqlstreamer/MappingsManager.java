package com.malt.mongopostgresqlstreamer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import com.malt.mongopostgresqlstreamer.model.Mappings;
import com.malt.mongopostgresqlstreamer.model.TableMapping;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class MappingsManager {

    @Value("${mappings:mappings.json}")
    private String mappingFile;

    Mappings mappingConfigs;

    @PostConstruct
    public void read() throws FileNotFoundException {
        mappingConfigs = read(mappingFile);
    }

    private Mappings read(String mappingFile) throws FileNotFoundException {
        Mappings mappingConfigs = new Mappings();
        List<DatabaseMapping> dbs = new ArrayList<>();

        JsonObject mappings = new JsonParser().parse(new FileReader(mappingFile)).getAsJsonObject();

        Set<String> databases = mappings.keySet();
        for (String dbName : databases) {
            List<TableMapping> tableMappings = new ArrayList<>();
            DatabaseMapping db = new DatabaseMapping();
            db.setTableMappings(tableMappings);
            db.setName(dbName);
            dbs.add(db);

            JsonObject database = mappings.getAsJsonObject(dbName);
            for (String collectionName : database.keySet()) {
                List<FieldMapping> fieldMappings = new ArrayList<>();
                List<String> indices = new ArrayList<>();
                TableMapping tableMapping = new TableMapping();
                tableMapping.setIndices(indices);
                tableMapping.setSourceCollection(collectionName);
                tableMapping.setDestinationName(collectionName);
                tableMappings.add(tableMapping);
                tableMapping.setFieldMappings(fieldMappings);
                JsonObject collection = database.getAsJsonObject(collectionName);
                tableMapping.setPrimaryKey(collection.get("pk").getAsString());
                if (collection.has("indices")) {
                    JsonArray listOfIndices = collection.get("indices").getAsJsonArray();
                    for (JsonElement index : listOfIndices) {
                        indices.add(index.getAsString());
                    }
                }

                addCreationDateGeneratedFieldDefinition(collectionName, fieldMappings, indices);

                for (String fieldName : collection.keySet()) {
                    if (!fieldName.equals("pk") && !fieldName.equals("indices")) {
                        JsonObject fieldObject = collection.getAsJsonObject(fieldName);
                        FieldMapping fieldMapping = new FieldMapping();
                        fieldMapping.setSourceName(fieldName);

                        if (fieldObject.has("dest")) {
                            fieldMapping.setDestinationName(fieldObject.get("dest").getAsString());
                        } else {
                            fieldMapping.setDestinationName(fieldMapping.getSourceName());
                        }

                        fieldMapping.setType(fieldObject.get("type").getAsString());
                        fieldMappings.add(fieldMapping);

                        if (fieldObject.has("index")) {
                            fieldMapping.setIndexed(fieldObject.get("index").getAsBoolean());
                            addToIndices(collectionName, indices, fieldMapping);
                        }

                        if (fieldObject.has("fk")) {
                            fieldMapping.setForeignKey(fieldObject.get("fk").getAsString());
                        }

                        if (fieldObject.has("valueField")) {
                            fieldMapping.setScalarFieldDestinationName(fieldObject.get("valueField").getAsString());
                        }
                    }
                }
                if (!tableMapping.getByDestinationName(tableMapping.getPrimaryKey()).isPresent()) {
                    tableMapping.getFieldMappings().add(
                            new FieldMapping(
                                    "", "id", "VARCHAR", true, null, null
                            )
                    );
                }
            }
        }
        mappingConfigs.setDatabaseMappings(dbs);

        checkIntegrity(mappingConfigs);

        return mappingConfigs;
    }

    private void checkIntegrity(Mappings mappingConfigs) {
        checkNoCollectionNameConflicts(mappingConfigs);
    }

    private void checkNoCollectionNameConflicts(Mappings mappingConfigs) {
        List<String> collectionNames = mappingConfigs.getDatabaseMappings()
                .stream()
                .flatMap(d -> d.getTableMappings().stream())
                .map(TableMapping::getDestinationName)
                .collect(Collectors.toList());
        Set<String> duplicates = collectionNames
                .stream()
                .filter(s -> Collections.frequency(collectionNames, s) > 1)
                .collect(Collectors.toSet());
        if (duplicates.size() > 0) {
            throw new IllegalStateException(String.format("Your mappings have several tables with the same name. " +
                    "It will lead to conflicts in your database. The culprits are %s", duplicates));
        }

    }

    private void addToIndices(String collectionName, List<String> indices, FieldMapping fieldMapping) {
        indices.add(String.format("INDEX idx_%s_%s ON %s (%s)",
                collectionName.replace(".", "_"),
                fieldMapping.getDestinationName(), collectionName, fieldMapping.getDestinationName()));
    }

    private void addCreationDateGeneratedFieldDefinition(String collectionName, List<FieldMapping> fieldMappings, List<String> indices) {
        FieldMapping creationDateDefinition = new FieldMapping();
        creationDateDefinition.setType("TIMESTAMP");
        creationDateDefinition.setDestinationName("_creationdate");
        creationDateDefinition.setIndexed(true);
        creationDateDefinition.setSourceName("_creationdate");
        fieldMappings.add(creationDateDefinition);
        addToIndices(collectionName, indices,  creationDateDefinition);
    }


}
