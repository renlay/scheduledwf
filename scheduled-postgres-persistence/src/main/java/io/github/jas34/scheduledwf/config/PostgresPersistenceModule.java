package io.github.jas34.scheduledwf.config;

import javax.sql.DataSource;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.conductor.postgres.PostgresConfiguration;
import com.netflix.conductor.postgres.PostgresDataSourceProvider;
import com.netflix.conductor.postgres.SystemPropertiesPostgresConfiguration;

import io.github.jas34.scheduledwf.dao.IndexScheduledWfDAO;
import io.github.jas34.scheduledwf.dao.ScheduledWfMetadataDAO;
import io.github.jas34.scheduledwf.dao.postgres.PostgresIndexScheduledWfDAO;
import io.github.jas34.scheduledwf.dao.postgres.PostgresScheduledWfMetaDataDao;

/**
 * @author Jasbir Singh
 */
public class PostgresPersistenceModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(PostgresConfiguration.class).to(SystemPropertiesPostgresConfiguration.class);
        bind(DataSource.class).toProvider(PostgresDataSourceProvider.class).in(Scopes.SINGLETON);
        bind(ScheduledWfMetadataDAO.class).to(PostgresScheduledWfMetaDataDao.class);
        bind(IndexScheduledWfDAO.class).to(PostgresIndexScheduledWfDAO.class);
    }
}
