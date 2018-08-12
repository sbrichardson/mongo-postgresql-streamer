package com.malt.mongopostgresqlstreamer;

import com.malt.mongopostgresqlstreamer.connectors.Connector;
import com.malt.mongopostgresqlstreamer.model.DatabaseMapping;
import com.malt.mongopostgresqlstreamer.model.FlattenMongoDocument;
import com.malt.mongopostgresqlstreamer.model.TableMapping;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

@Service
public class InitialImporter {

    @Autowired
    private MappingsManager mappingsManager;
    @Autowired
    @Qualifier("database")
    private MongoDatabase mongoDatabase;
    @Autowired
    private List<Connector> connectors;

    public void start() {
        createSchema();
        populateData();
    }

    private void populateData() {
        DatabaseMapping mappingConfigs = mappingsManager.mappingConfigs.getDatabaseMappings().get(0);
        List<String> collectionNames = toStream(mongoDatabase.listCollectionNames().iterator()).collect(toList());

        for (Connector connector : connectors) {
            for (TableMapping tableMapping : mappingConfigs.getTableMappings()) {
                boolean needToBeImported = collectionNames.stream()
                        .anyMatch(collectionIsMapped(tableMapping));
                if (needToBeImported) {
                    connector.bulkInsert(
                            tableMapping.getSourceCollection(),
                            mongoDatabase.getCollection(tableMapping.getSourceCollection()).count(),
                            toStream(
                                    mongoDatabase.getCollection(tableMapping.getSourceCollection())
                                            .find()
                                            .noCursorTimeout(true)
                                            .iterator()
                            )
                                    .map(FlattenMongoDocument::fromMap),
                            mappingConfigs
                    );
                }
            }
        }

    }

    @Transactional
    protected void createSchema() {
        DatabaseMapping mappingConfigs = mappingsManager.mappingConfigs.getDatabaseMappings().get(0);
        List<String> collectionNames = toStream(mongoDatabase.listCollectionNames().iterator()).collect(toList());

        for (Connector connector : connectors) {
            for (TableMapping tableMapping : mappingConfigs.getTableMappings()) {

                boolean needToBeImported = collectionNames
                                                    .stream()
                                                    .anyMatch(collectionIsMapped(tableMapping));
                if (needToBeImported) {
                    connector.prepareInitialImport(tableMapping.getSourceCollection(), mappingConfigs);
                }
            }
        }

    }

    private Predicate<String> collectionIsMapped(TableMapping tableMapping) {
        return collectionName -> collectionName.equals(tableMapping.getSourceCollection());
    }

    private <T> Stream<T> toStream(MongoCursor<T> iterator) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
        );
    }
}
