package com.malt.mongopostgresqlstreamer.model;

import lombok.Data;

import java.util.List;

@Data
public class Mappings {
    private List<DatabaseMapping> databaseMappings;
}
