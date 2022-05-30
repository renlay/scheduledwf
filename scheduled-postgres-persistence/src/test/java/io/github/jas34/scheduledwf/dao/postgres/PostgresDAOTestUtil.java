package io.github.jas34.scheduledwf.dao.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.postgres.config.PostgresProperties;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.time.Duration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Jasbir Singh
 */
public class PostgresDAOTestUtil {

    private final HikariDataSource dataSource;
    private final PostgresProperties properties = mock(PostgresProperties.class);
    private final ObjectMapper objectMapper;

    public PostgresDAOTestUtil(PostgreSQLContainer<?> postgreSQLContainer, ObjectMapper objectMapper) {

        this.objectMapper = objectMapper;

        this.dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(postgreSQLContainer.getJdbcUrl());
        dataSource.setUsername(postgreSQLContainer.getUsername());
        dataSource.setPassword(postgreSQLContainer.getPassword());
        dataSource.setAutoCommit(false);

        when(properties.getTaskDefCacheRefreshInterval()).thenReturn(Duration.ofSeconds(60));

        // Prevent DB from getting exhausted during rapid testing
        dataSource.setMaximumPoolSize(8);

        flywayMigrate(dataSource);
    }

    private void flywayMigrate(DataSource dataSource) {
        FluentConfiguration fluentConfiguration = Flyway.configure()
                .table("schema_version")
                .dataSource(dataSource)
                .placeholderReplacement(false);

        Flyway flyway = fluentConfiguration.load();
        flyway.migrate();
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public PostgresProperties getTestProperties() {
        return properties;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

}
