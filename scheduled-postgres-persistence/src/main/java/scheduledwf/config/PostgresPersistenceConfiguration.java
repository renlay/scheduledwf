package io.github.jas34.scheduledwf.config;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jas34.scheduledwf.dao.IndexScheduledWfDAO;
import io.github.jas34.scheduledwf.dao.ScheduledWfMetadataDAO;
import io.github.jas34.scheduledwf.dao.postgres.PostgresIndexScheduledWfDAO;
import io.github.jas34.scheduledwf.dao.postgres.PostgresScheduledWfMetaDataDao;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;

/**
 * Description:<br>
 * Date: 26/09/21-5:13 pm
 * @since v2.0.0
 * @author Jasbir Singh
 */
@Configuration
@Import(DataSourceAutoConfiguration.class)
@ConditionalOnProperty(name = "conductor.db.type", havingValue = "postgres")
public class PostgresPersistenceConfiguration {
	@Bean
	@DependsOn({"flyway", "flywayInitializer"})
	public ScheduledWfMetadataDAO scheduledWfMetadataDAO(ObjectMapper objectMapper, DataSource dataSource) {
		return new PostgresScheduledWfMetaDataDao(objectMapper, dataSource);
	}

	@Bean
	@DependsOn({"flyway", "flywayInitializer"})
	public IndexScheduledWfDAO indexScheduledWfDAO(ObjectMapper objectMapper, DataSource dataSource) {
		return new PostgresIndexScheduledWfDAO(objectMapper, dataSource);
	}
}
