package com.malt.mongopostgresqlstreamer.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(value = "com.malt.mongopostgresqlstreamer")
@EnableAutoConfiguration
public class StreamerTestConfig {
}
