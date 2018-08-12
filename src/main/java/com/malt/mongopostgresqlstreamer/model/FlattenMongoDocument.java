package com.malt.mongopostgresqlstreamer.model;

import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;
import lombok.Data;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Data
public class FlattenMongoDocument {
    private Map<String, Object> values;

    public static FlattenMongoDocument fromMap(Map map) {
        FlattenMongoDocument flattenMongoDocument = new FlattenMongoDocument();
        flattenMongoDocument.setValues(map);
        return flattenMongoDocument;
    }


    public static FlattenMongoDocument fromDocument(Document document) {
        FlattenMongoDocument flattenMongoDocument = new FlattenMongoDocument();
        flattenMongoDocument.setValues(
                filterOid(
                        new JsonFlattener(document.toJson())
                                .withFlattenMode(FlattenMode.KEEP_ARRAYS)
                                .flattenAsMap()
                )
        );

        return flattenMongoDocument;
    }

    private static Map<String, Object> filterOid(Map<String, Object> flattenAsMap) {
        Map<String, Object> filteredMap = new HashMap<>();
        flattenAsMap.forEach((k, v) ->
                filteredMap.put(k.replace(".$oid", ""), v));
        return filteredMap;
    }

    public Optional<Object> get(String key) {
        return Optional.ofNullable(values.getOrDefault(key, null));
    }

    public FlattenMongoDocument withField(String key, Object value) {
        this.values.put(key, value);
        return this;
    }
}
