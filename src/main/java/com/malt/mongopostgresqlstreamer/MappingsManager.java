package com.malt.mongopostgresqlstreamer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import com.malt.mongopostgresqlstreamer.model.Mappings;
import com.malt.mongopostgresqlstreamer.model.TableMapping;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class MappingsManager {

    Mappings mappingConfigs;

    @PostConstruct
    public void read() throws FileNotFoundException {
        mappingConfigs = read("mappings.json");
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
                            indices.add(String.format("INDEX idx_%s_%s ON %s (%s)",
                                    collectionName.replace(".", "_"),
                                    fieldMapping.getDestinationName(), collectionName, fieldMapping.getDestinationName()));
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

        return mappingConfigs;
    }


}
