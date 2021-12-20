package org.flywaydb.community.database.mongodb;

import java.sql.SQLException;
import java.util.List;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

public class MongoCollection extends Table<MongoDatabase, MongoSchema> {

    /**
     * @param jdbcTemplate The JDBC template for communicating with the DB.
     * @param database     The database-specific support.
     * @param schema       The schema this table lives in.
     * @param name         The name of the table.
     */
    public MongoCollection(JdbcTemplate jdbcTemplate, MongoDatabase database, MongoSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("db." + name + ".drop()");
    }

    @Override
    protected boolean doExists() throws SQLException {
        List<String> names = jdbcTemplate.queryForStringList("db.listCollectionNames()");
        return names.contains(name);
    }

    @Override
    protected void doLock() throws SQLException {
    }
}
