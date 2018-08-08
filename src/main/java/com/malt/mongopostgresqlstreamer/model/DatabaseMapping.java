package com.malt.mongopostgresqlstreamer.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
public class DatabaseMapping {

    private String name;

    private List<TableMapping> tableMappings = new ArrayList<>();

    public Optional<TableMapping> get(String sourceCollection) {
        return tableMappings.stream()
                .filter(tableMapping -> tableMapping.getSourceCollection().equals(sourceCollection))
                .findFirst();
    }
}
