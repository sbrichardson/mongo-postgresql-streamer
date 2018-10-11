package com.malt.mongopostgresqlstreamer.config;

import com.malt.mongopostgresqlstreamer.StreamerApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

@Configuration
@ComponentScan(value = "com.malt.mongopostgresqlstreamer")
@EnableAutoConfiguration
public class StreamerTestConfig {
}
