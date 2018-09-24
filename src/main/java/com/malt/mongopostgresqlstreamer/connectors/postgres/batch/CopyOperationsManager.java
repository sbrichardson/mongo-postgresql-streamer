package com.malt.mongopostgresqlstreamer.connectors.postgres.batch;

import com.malt.mongopostgresqlstreamer.connectors.postgres.Field;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class CopyOperationsManager {
    private final CopyManager copyManager;
    private final Map<String, SingleTableCopyOperations> copyOperationsPerTable = new HashMap<>();

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
                            copyManager
                    )
            );
        }

        copyOperationsPerTable.get(table).addOperation(fields);
    }

    public void finalizeCopyOperations(String destTable) {
        SingleTableCopyOperations operations = copyOperationsPerTable.get(destTable);
        if (operations != null) {
            operations.finalizeOperations();
        }
    }
}
