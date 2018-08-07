package com.malt.mongopostgresqlstreamer.model;

import lombok.Data;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Data
public class Mappings {

    private List<Database> databases;

    public List<Table> listOfTables() {
        return databases
                .stream()
                .flatMap(d -> d.getTables().stream())
                .collect(toList());
    }
}
