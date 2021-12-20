package org.flywaydb.community.database.mongodb;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

public class MongoSchema extends Schema<MongoDatabase, MongoCollection> {

    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public MongoSchema(JdbcTemplate jdbcTemplate, MongoDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return true;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return false;
    }

    @Override
    protected void doCreate() throws SQLException {

    }

    @Override
    protected void doDrop() throws SQLException {


    }

    @Override
    protected void doClean() throws SQLException {

    }

    @Override
    protected MongoCollection[] doAllTables() throws SQLException {
        List<String> tables = jdbcTemplate.queryForStringList("db.getCollectionNames()");
        return tables.stream().map(tableName -> new MongoCollection(jdbcTemplate, database, this, tableName)).toArray(MongoCollection[]::new);
    }

    @Override
    public Table getTable(String tableName) {
        return new MongoCollection(jdbcTemplate, database, this, tableName);
    }
}
