package com.malt.mongopostgresqlstreamer;

import com.malt.mongopostgresqlstreamer.model.Mappings;
import com.malt.mongopostgresqlstreamer.model.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

import static java.lang.String.format;

@Service
public class InitialImporter {

    @Autowired
    private MappingsManager mappingsManager;
    @Autowired
    JdbcTemplate jdbcTemplate;

    public void start() {
        createSchema();
        populateData();
    }

    private void populateData() {
    }

    private String fieldAndTypes(Table table) {
        return table.getFields()
                .stream()
                .map(f -> f.getDest() + " " + f.getType())
                .collect( Collectors.joining( "," ));
    }

    private void createSchema() {
        Mappings mappingConfigs = mappingsManager.mappingConfigs;
        for (Table table : mappingConfigs.listOfTables()) {
            jdbcTemplate.execute(format("DROP TABLE IF EXISTS %s", table.getName()));
            jdbcTemplate.execute(format("CREATE TABLE %s (%s)", table.getName(), fieldAndTypes(table)));
            jdbcTemplate.execute(format("ALTER TABLE %s ADD PRIMARY KEY(%s)", table.getName(), table.getPk()));
        }
    }
}
