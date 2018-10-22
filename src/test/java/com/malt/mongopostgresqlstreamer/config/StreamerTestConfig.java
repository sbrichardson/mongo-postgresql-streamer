package com.malt.mongopostgresqlstreamer.config;

import com.malt.mongopostgresqlstreamer.StreamerApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.*;

@Configuration
@ComponentScan(
        value = "com.malt.mongopostgresqlstreamer",
        excludeFilters = {
                @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = StreamerApplication.class)
        })
@EnableAutoConfiguration
public class StreamerTestConfig {
}
