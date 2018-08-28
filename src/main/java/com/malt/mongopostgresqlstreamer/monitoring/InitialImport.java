package com.malt.mongopostgresqlstreamer.monitoring;

import lombok.Data;

import java.util.Date;

@Data
public class InitialImport {
    private Double lengthInMinutes;
    private Date start;
    private Date end;
    private String status;
}
