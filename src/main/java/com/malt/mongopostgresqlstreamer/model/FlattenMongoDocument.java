package com.malt.mongopostgresqlstreamer.model;

import lombok.Data;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.Map;
import java.util.Optional;

@Data
public class FlattenMongoDocument {
    private Map<String, Object> values;

    public static FlattenMongoDocument fromMap(Map map) {
        FlattenMongoDocument flattenMongoDocument = new FlattenMongoDocument();
        flattenMongoDocument.setValues(map);
        addCreationDateIfPossible(flattenMongoDocument);

        return flattenMongoDocument;
    }

    private static void addCreationDateIfPossible(FlattenMongoDocument flattenMongoDocument) {
        flattenMongoDocument.get("_id").ifPresent( id -> {
            if (id instanceof ObjectId) {
                Date creationDate = ((ObjectId) id).getDate();
                flattenMongoDocument.getValues().put("_creationdate", creationDate);
            }
        });
    }

    public Optional<Object> get(String key) {
        return Optional.ofNullable(values.getOrDefault(key, null));
    }

    public FlattenMongoDocument withField(String key, Object value) {
        this.values.put(key, value);
        return this;
    }
}
