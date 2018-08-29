package com.malt.mongopostgresqlstreamer.connectors;

import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.FlattenMongoDocument;

import java.util.stream.Stream;

public interface Connector {
    void createTable(
            String sourceCollection,
            DatabaseMapping mapping
    );

    void insert(String collection, FlattenMongoDocument document, DatabaseMapping mappings);

    void addConstraints(
            String sourceCollection,
            DatabaseMapping mapping
    );

    void upsert(String collection, FlattenMongoDocument document, DatabaseMapping mappings);

    void update(String collection, FlattenMongoDocument document, DatabaseMapping mappings);

    void remove(String collection, FlattenMongoDocument document, DatabaseMapping mappings);

    void bulkInsert(String collection, long totalNumberOfDocuments, Stream<FlattenMongoDocument> documents, DatabaseMapping mappings);
}
