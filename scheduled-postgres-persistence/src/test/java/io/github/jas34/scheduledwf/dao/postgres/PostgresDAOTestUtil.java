package io.github.jas34.scheduledwf.dao.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.config.Configuration;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * @author Jasbir Singh
 */
public class PostgresDAOTestUtil {
    private static final Logger logger = LoggerFactory.getLogger(PostgresDAOTestUtil.class);
    private final HikariDataSource dataSource;
    private final TestConfiguration testConfiguration = new TestConfiguration();
    private final ObjectMapper objectMapper = new JsonMapperProvider().get();

    PostgresDAOTestUtil(String dbName) throws Exception {
        // dbName = conductor_unit_test
        // String normalizedDbName = dbName.toLowerCase();
        testConfiguration.setProperty("jdbc.url", "jdbc:mysql://localhost:3306/" + dbName
                + "?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");
        testConfiguration.setProperty("jdbc.username", "root");
        testConfiguration.setProperty("jdbc.password", "test#1234");
        createDatabase(dbName);
        this.dataSource = getDataSource(testConfiguration);
    }

    private void createDatabase(String dbName) {

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
        dataSource.setUsername("root");
        dataSource.setPassword("test#1234");
        dataSource.setAutoCommit(false);

        dataSource.setMaximumPoolSize(2);

        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
            }
        } catch (SQLException sqlException) {
            logger.error("Unable to create default connection for docker mysql db", sqlException);
            throw new RuntimeException(sqlException);
        } finally {
            dataSource.close();
        }
    }

    private HikariDataSource getDataSource(Configuration config) {

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
        dataSource.setUsername(config.getProperty("jdbc.username", "conductor"));
        dataSource.setPassword(config.getProperty("jdbc.password", "password"));
        dataSource.setAutoCommit(false);

        // Prevent DB from getting exhausted during rapid testing
        dataSource.setMaximumPoolSize(8);

        flywayMigrate(dataSource);

        return dataSource;
    }

    private void flywayMigrate(DataSource dataSource) {

        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setBaselineOnMigrate(true);
        flyway.setPlaceholderReplacement(false);
        flyway.migrate();
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public TestConfiguration getTestConfiguration() {
        return testConfiguration;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void resetAllData() {
        logger.info("Resetting data for test");
        try (Connection connection = dataSource.getConnection()) {
            try (ResultSet rs = connection.prepareStatement("SHOW TABLES").executeQuery();
                    PreparedStatement keysOn = connection.prepareStatement("SET FOREIGN_KEY_CHECKS=1")) {
                try (PreparedStatement keysOff = connection.prepareStatement("SET FOREIGN_KEY_CHECKS=0")) {
                    keysOff.execute();
                    while (rs.next()) {
                        String table = rs.getString(1);
                        try (PreparedStatement ps = connection.prepareStatement("TRUNCATE TABLE " + table)) {
                            ps.execute();
                        }
                    }
                } finally {
                    keysOn.execute();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
