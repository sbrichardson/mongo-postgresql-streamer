package com.malt.mongopostgresqlstreamer.connectors.postgres;

import com.malt.mongopostgresqlstreamer.connectors.postgres.batch.CopyOperationsManager;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@Service
@Slf4j
public class SqlExecutor {
    private final JdbcTemplate jdbcTemplate;
    private final CopyOperationsManager copyOperationsManager;

    @Inject
    public SqlExecutor(JdbcTemplate jdbcTemplate, CopyOperationsManager copyOperationsManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.copyOperationsManager = copyOperationsManager;
    }

    void upsert(String table, String primaryKey, List<Field> fields) {
        String commaSeparatedFieldNames = getCommaSeparatedFieldNames(fields);
        String placeholders = getPlaceholders(fields);
        String query = format(
                "INSERT INTO %s (%s) VALUES(%s) ON CONFLICT (%s) DO UPDATE SET %s",
                table, commaSeparatedFieldNames, placeholders,
                primaryKey, generateUpdateString(fields)
        );

        List<Object> values = getValues(fields);
        values.addAll(values); // Duplicates are needed for the UPDATE clause
        Object[] valuesArray = values.toArray();

        log.debug("{} {}", query, valuesArray);
        try {
            jdbcTemplate.update(query, valuesArray);
        } catch (Exception e) {
            log.error("Unable to upsert record with values : {}", getValues(fields), e);
            throw e;
        }
    }

    void batchInsert(String table, List<FieldMapping> mappings, List<Field> fields) {
        log.trace("Bulking insert of {} ({})", table, fields);
        copyOperationsManager.addInsertOperation(table, mappings, fields);
    }

    public void finalizeBatchInsert(String destTable) {
        copyOperationsManager.finalizeCopyOperations(destTable);
    }

    void dropTable(String table) {
        log.debug("Dropping table '{}'...", table);
        sqlExecute("DROP TABLE IF EXISTS %s", table);
    }

    void setTableAsLogged(String table) {
        log.debug("Set table '{}' as logged ...", table);
        sqlExecute("ALTER TABLE %s SET LOGGED", table);
    }

    void addPrimaryKey(String table, String primaryKeyName) {
        log.debug("Adding primary key '{}' on table '{}' ...", primaryKeyName, table);
        sqlExecute("ALTER TABLE %s ADD PRIMARY KEY(%s)", table, primaryKeyName);
    }

    void createTable(String table, List<FieldMapping> fieldMappings) {
        String formattedFields = fieldAndTypes(fieldMappings);
        log.debug("Creating table '{}' with fields {}", table, formattedFields);
        sqlExecute("CREATE UNLOGGED TABLE %s (%s)", table, formattedFields);
    }

    void createIndex(String index) {
        log.debug("Creating index '{}'", index);
        sqlExecute("CREATE " + index);
    }

    void remove(String table, String primaryKey, Object primaryKeyValue) {
        log.debug("Remove document where '{} = {}' from {}", primaryKey, primaryKeyValue, table);

        try {
            jdbcTemplate.update(
                    format("DELETE FROM %s WHERE %s = ? ", table, primaryKey),
                    primaryKeyValue
            );
        } catch (Exception e) {
            log.error("Unable to delete record : {}", primaryKey, e);
            throw e;
        }
    }

    private void sqlExecute(String query, Object... parameters) {
        String sql = format(query, parameters);

        log.trace(sql);
        jdbcTemplate.execute(sql);
    }


    private List<Object> getValues(List<Field> fields) {
        return fields.stream()
                .map(Field::getValue)
                .map(v -> v instanceof String ? ((String) v).replaceAll("\\u0000","") : v)
                .map(v -> v instanceof ObjectId ? v.toString() : v)
                .collect(toList());
    }

    private String getPlaceholders(List<Field> fields) {
        return String.join(
                ", ",
                fields.stream().map(p -> "?").collect(toList())
        );
    }

    private String getCommaSeparatedFieldNames(List<Field> fields) {
        return String.join(
                ", ",
                fields.stream().map(Field::getName).collect(toList())
        );
    }

    private String fieldAndTypes(List<FieldMapping> fieldMappings) {
        return fieldMappings
                .stream()
                .filter(f -> !f.getType().startsWith("_"))
                .sorted(Comparator.comparing(FieldMapping::getDestinationName))
                .map(f -> f.getDestinationName() + " " + f.getType())
                .collect(Collectors.joining(","));
    }

    private String generateUpdateString(List<Field> fields) {
        return String.join(
                ", ",
                fields.stream()
                        .map(field -> field.getName() + " = ?")
                        .collect(toList())
        );
    }
}
