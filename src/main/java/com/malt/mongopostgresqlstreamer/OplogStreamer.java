package com.malt.mongopostgresqlstreamer;

import com.malt.mongopostgresqlstreamer.connectors.Connector;
import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.FlattenMongoDocument;
import com.mongodb.CursorType;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.Filters.*;

@Service
@Slf4j
public class OplogStreamer {

    @Value(value = "${mongo.connector.identifier:streamer}")
    private String identifier;

    @Value(value = "${mongo.database:test}")
    private String dbName;

    @Autowired
    private MappingsManager mappingsManager;
    @Autowired
    private CheckpointManager checkpointManager;
    @Autowired
    @Qualifier("oplog")
    private MongoDatabase oplog;
    @Autowired
    @Qualifier("database")
    private MongoDatabase database;
    @Autowired
    private List<Connector> connectors;

    public void watchFromCheckpoint(Optional<BsonTimestamp> checkpoint) {
        log.info("Start watching the oplog...");
        for (Document document : oplog.getCollection("oplog.rs").find(oplogfilters(checkpoint)).cursorType(CursorType.TailableAwait)) {
            BsonTimestamp timestamp = processOperation(document);
            checkpointManager.keep(timestamp);
        }
    }

    @Transactional
    BsonTimestamp processOperation(Document document) {
        String namespace = document.getString("ns");
        // TODO retrieve database as well
        String collection = namespace.split("\\.")[1];
        String operation = document.getString("op");
        BsonTimestamp timestamp = document.get("ts", BsonTimestamp.class);

        DatabaseMapping mappings = mappingsManager.mappingConfigs.getDatabaseMappings().get(0);
        // TODO this test could lead to unexpected behaviour
        // if we have two collection with the same name
        // but in different database
        if (mappings.get(collection).isPresent()) {
            log.debug("Operation {} detected on {}", operation, namespace);
            switch (operation) {
                case "i":
                    Map newDocument = (Map) document.get("o");
                    connectors.forEach(connector ->
                            connector.insert(
                                    collection,
                                    FlattenMongoDocument.fromMap(newDocument),
                                    mappings
                            )
                    );
                    break;
                case "u":
                    Map documentIdToUpdate = (Map) document.get("o2");
                    Document updatedDocument = database.getCollection(collection)
                            .find(eq("_id", documentIdToUpdate.get("_id")))
                            .first();
                    if (updatedDocument != null) {
                        connectors.forEach(connector ->
                                connector.update(
                                        collection,
                                        FlattenMongoDocument.fromMap(updatedDocument),
                                        mappings
                                )
                        );
                    }
                    break;
                case "d":
                    Map documentIdToRemove = (Map) document.get("o");
                    connectors.forEach(connector ->
                            connector.remove(
                                    collection,
                                    FlattenMongoDocument.fromMap(documentIdToRemove),
                                    mappings
                            )
                    );
                    break;
                default:
                    break;
            }
        }
        return timestamp;
    }

    private void remove(Document document) {
        Object id = document.get("o");
        System.out.println("Remove document by id : " + id);
    }

    private void update(Document document) {

    }

    private void insert(Document document) {
        Object doc = document.get("o");
        System.out.println("Insert new doc : " + doc);
    }


    private Bson oplogfilters(Optional<BsonTimestamp> checkpoint) {
        if (checkpoint.isPresent()) {
            return and(
                    gt("ts", checkpoint.get()),
                    ne("ns", database.getName() + ".mongooplog"),
                    regex("ns", database.getName()));
        }
        return and(
                ne("ns", database.getName() + ".mongooplog"),
                regex("ns", database.getName()));
    }


}
