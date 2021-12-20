package org.flywaydb.community.database.mongodb;

import java.sql.SQLException;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;

public class MongoConnection extends Connection<MongoDatabase> {

    protected MongoConnection(MongoDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return getJdbcTemplate().queryForString("db.getName()");
    }

    @Override
    public Schema getSchema(String name) {
        return new MongoSchema(getJdbcTemplate(), database, name);
    }
}
