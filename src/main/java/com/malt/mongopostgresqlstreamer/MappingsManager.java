package com.malt.mongopostgresqlstreamer;

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
                Table table = new Table();
                table.setName(collectionName);
                tables.add(table);
                table.setFields(fields);
                JsonObject collection = database.getAsJsonObject(collectionName);
                table.setPk(collection.get("pk").getAsString());
                for (String fieldName : collection.keySet()) {
                    if (!fieldName.equals("pk")) {
                        JsonObject fieldObject = collection.getAsJsonObject(fieldName);
                        Field field = new Field();
                        field.setDest(fieldObject.get("dest").getAsString());
                        field.setType(fieldObject.get("type").getAsString());
                        fields.add(field);
                    }
                }
            }
        }
        mappingConfigs.setDatabases(dbs);

        return mappingConfigs;
    }


}
