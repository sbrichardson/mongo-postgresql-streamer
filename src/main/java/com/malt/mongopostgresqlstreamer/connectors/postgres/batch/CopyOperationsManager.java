package com.malt.mongopostgresqlstreamer.connectors.postgres.batch;

import com.malt.mongopostgresqlstreamer.connectors.postgres.Field;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class CopyOperationsManager {
    private static final int MAX_ELEMENTS_BEFORE_CALLBACK = 5000;
    private static final int MAX_SECONDS_BEFORE_CALLBACK = 10;

    private final CopyManager copyManager;
    private final Map<String, SingleTableCopyOperations> copyOperationsPerTable = new ConcurrentHashMap<>();

    @Inject
    public CopyOperationsManager(CopyManager copyManager) {
        this.copyManager = copyManager;
    }

    public void addInsertOperation(String table, List<FieldMapping> fieldMappings, List<Field> fields) {
        if (!copyOperationsPerTable.containsKey(table)) {
            copyOperationsPerTable.put(
                    table,
                    new SingleTableCopyOperations(
                            table, fieldMappings,
                            MAX_SECONDS_BEFORE_CALLBACK, MAX_ELEMENTS_BEFORE_CALLBACK,
                            this::commitPendingUpserts
                    )
            );
        }

        copyOperationsPerTable.get(table).addOperation(fields);
    }


    private void commitPendingUpserts(SingleTableCopyOperations singleTableCopyOperations) {
        try {
            log.trace("COPY on {} : {}", singleTableCopyOperations.getTable(), singleTableCopyOperations.getCopyContent());
            copyManager.copyIn(
                    "COPY " + singleTableCopyOperations.getTable() + " FROM STDIN WITH DELIMITER ',' NULL as 'null' CSV HEADER",
                    singleTableCopyOperations.getCopyContentStream()
            );
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
