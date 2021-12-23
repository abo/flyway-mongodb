package org.flywaydb.community.database.mongodb;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.flywaydb.core.api.MigrationType;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.AbbreviationUtils;

public class MongoDatabase extends Database<MongoConnection> {

    public MongoDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected MongoConnection doGetConnection(Connection connection) {
        return new MongoConnection(this, connection);
    }

    @Override
    public void ensureSupported() {

    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public boolean supportsChangingCurrentSchema() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "true";
    }

    @Override
    public String getBooleanFalse() {
        return "false";
    }

    @Override
    public boolean catalogIsSchema() {
        return true;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String result = "db.createCollection(\"" + table.getName() + "\", {validator: { $jsonSchema: { bsonType: \"object\", required: [ \"installed_rank\", \"description\", \"type\", \"script\", \"installed_by\", \"installed_on\", \"execution_time\", \"success\" ] } }});";

        if (baseline) {
            String installedBy = configuration.getInstalledBy() == null ? getCurrentUser() : configuration.getInstalledBy();
            result += String.format(getInsertStatement(table).replace("?", "%s"),
                    0,
                    "'" + configuration.getBaselineVersion() + "'",
                    "'" + AbbreviationUtils.abbreviateDescription(configuration.getBaselineDescription()) + "'",
                    "'" + MigrationType.BASELINE + "'",
                    "'" + AbbreviationUtils.abbreviateScript(configuration.getBaselineDescription()) + "'",
                    "null",
                    "'" + installedBy + "'",
                    0,
                    getBooleanTrue()
            );
        }
        return result;
    }

    @Override
    protected String doGetCurrentUser() throws SQLException {
        return super.doGetCurrentUser();
    }

    @Override
    public String getInsertStatement(Table table) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return "db." + table.getName() + ".insertOne({installed_rank: ?, " +
                "version: ?, " +
                "description: ?, " +
                "type: ?, " +
                "script: ?, " +
                "checksum: ?, " +
                "installed_by: ?, " +
                "installed_on: ISODate(\"" + df.format(new Date())+ "\"), " +
                "execution_time: ?, " +
                "success: ?})";
    }

    @Override
    public String getSelectStatement(Table table) {
        return "db." + table.getName() + ".find({type:{$ne:\"TABLE\"}, installed_rank:{$gt: ? }}).sort({installed_rank:1})";
    }

    @Override
    public String getUpdateStatement(Table table, int installedRank) {
        return "db." + table.getName() + ".updateOne({installed_rank: " + installedRank + "}, {$set:{description: ?, type: ?, checksum: ? }});";
//        return "UPDATE " + table.getName()
//                + " SET "
//                + quote("description") + "=? , "
//                + quote("type") + "=? , "
//                + quote("checksum") + "=?"
//                + " WHERE " + quote("installed_rank") + "=?";
//        return super.getUpdateStatement(table);
    }

    @Override
    public String getDeleteStatementForRepeatable(Table table) {
        return "db." + table.getName() + ".deleteOne({success: false, description: ?})";
    }

    @Override
    public String getDeleteStatementForVersioned(Table table) {
        return "db." + table.getName() + ".deleteOne({success: false, version: ?})";
    }
}
