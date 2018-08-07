package com.malt.mongopostgresqlstreamer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.malt.mongopostgresqlstreamer.model.Database;
import com.malt.mongopostgresqlstreamer.model.Field;
import com.malt.mongopostgresqlstreamer.model.Mappings;
import com.malt.mongopostgresqlstreamer.model.Table;
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
        List<Database> dbs = new ArrayList<>();

        JsonObject mappings = new JsonParser().parse(new FileReader(mappingFile)).getAsJsonObject();

        Set<String> databases = mappings.keySet();
        for (String dbName : databases) {
            List<Table> tables = new ArrayList<>();
            Database db = new Database();
            db.setTables(tables);
            db.setName(dbName);
            dbs.add(db);

            JsonObject database = mappings.getAsJsonObject(dbName);
            for (String collectionName : database.keySet()) {
                List<Field> fields = new ArrayList<>();
                List<String> indices = new ArrayList<>();
                Table table = new Table();
                table.setIndices(indices);
                table.setName(collectionName);
                tables.add(table);
                table.setFields(fields);
                JsonObject collection = database.getAsJsonObject(collectionName);
                table.setPk(collection.get("pk").getAsString());
                if (collection.has("indices")) {
                    JsonArray listOfIndices = collection.get("indices").getAsJsonArray();
                    for (JsonElement index : listOfIndices) {
                        indices.add(index.getAsString());
                    }
                }
                for (String fieldName : collection.keySet()) {
                    if (!fieldName.equals("pk") && !fieldName.equals("indices") ) {
                        JsonObject fieldObject = collection.getAsJsonObject(fieldName);
                        Field field = new Field();
                        field.setDest(fieldObject.get("dest").getAsString());
                        field.setType(fieldObject.get("type").getAsString());
                        fields.add(field);

                        if (fieldObject.has("index")) {
                            field.setIndexed(fieldObject.get("index").getAsBoolean());
                            indices.add(String.format("INDEX idx_%s_%s ON %s (%s)",
                                    collectionName.replace(".", "_"),
                                    field.getDest(), collectionName, field.getDest()));
                        }
                    }
                }
            }
        }
        mappingConfigs.setDatabases(dbs);

        return mappingConfigs;
    }


}
