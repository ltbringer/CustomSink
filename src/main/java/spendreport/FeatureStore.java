package spendreport;

import java.sql.*;
import java.util.Collections;
import java.util.UUID;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class FeatureStore extends RichSinkFunction<Feature>{
    private Connection connection;
    private PreparedStatement preparedStatement;
    private String dbSource;
    private String tableName;
    private final String[] columns = {"id", "data", "ts", "idempotence_key"};
    private final String[] dTypes = {"integer", "blob", "timestamp", "text"};
    private final String sqlitePrefix = "jdbc:sqlite:";

    private int ctr = 0;
    private final UUID id = UUID.randomUUID();

    public FeatureStore setSrc(String dbSource) {
        if (dbSource.startsWith(sqlitePrefix)) {
            dbSource = dbSource.substring(sqlitePrefix.length());
        }
        this.dbSource = dbSource;
        return this;
   }

    public FeatureStore setTable(String tableName) {
       this.tableName = tableName;
       return this;
    }

    private String sqlBuilder() {
        String columnString = String.join(", ", columns);
        String valuePlaceholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        return String.format("insert into %s (%s) values (%s)", tableName, columnString, valuePlaceholders);
    }

    private boolean isTableMissing() throws SQLException {
        String sql = String.format("select count(*) from sqlite_master where type='table' and name='%s'", tableName);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        rs.next();
        return rs.getInt(1) == 0;
    }

    private void createTable() throws SQLException {
        StringBuilder schemaString = new StringBuilder();
        String delimiter;
        for (int i = 0; i < columns.length; i++) {
            delimiter = i < columns.length - 1 ? ", " : "";
            schemaString.append(String.format("%s %s%s", columns[i], dTypes[i], delimiter));
        }
        String sql = String.format("create table %s (%s)", tableName, schemaString);
        Statement statement = connection.createStatement();
        statement.execute(sql);
        connection.close();
    }

    @Override
    public void open(Configuration parameters) throws SQLException {
        ctr += 1;
        String message = String.format("Opening connection for (%s) to %s, times: %d", id, dbSource, ctr);
        System.out.println(message);
        connection = DriverManager.getConnection(String.format("%s%s", sqlitePrefix, dbSource));
        if (isTableMissing()) {
            createTable();
        }
        preparedStatement = connection.prepareStatement(sqlBuilder());
    }

    @Override
    public void invoke(Feature feature, Context context) throws Exception {
        String message = String.format("Sinking feature (%s) for (%s)", feature, id);
        System.out.println(message);
        preparedStatement.setLong(1, feature.getId());
        preparedStatement.setBytes(2, feature.getData());
        preparedStatement.setTimestamp(3, new Timestamp(feature.getTs()));
        preparedStatement.setString(4, feature.getKey().toString());
        preparedStatement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
