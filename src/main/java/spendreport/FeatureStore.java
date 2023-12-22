package spendreport;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

public class FeatureStore implements SinkWriter<Row> {
    private final Connection connection;
    private final PreparedStatement insertStatement;
    private final String tableName;
    private final String dbSource;
    private final String sqlitePrefix = "jdbc:sqlite:";
    private final String[] columns = {"id", "data", "ts"};
    private final String[] dTypes = {"integer", "blob", "timestamp"};
    private transient Instant time = Instant.now();

    private final UUID id = UUID.randomUUID();

    public FeatureStore(String tableName, String dbSource) throws IOException {
        this.tableName = tableName;
        this.dbSource = dbSource;
        try {
            this.createTableIfNotExists();
            this.connection = DriverManager.getConnection(sqlitePrefix + dbSource);
            this.connection.setAutoCommit(false);
            this.insertStatement = connection.prepareStatement(insertSQLBuilder());
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private String updateSQLBuilder() {
        // all columns except 'id'
        String[] nonIdCols = Arrays.copyOfRange(columns, 1, columns.length);
        String columnString = String.join(", ", nonIdCols);
        String valuePlaceholders = String.join(", ", Collections.nCopies(nonIdCols.length, "?"));
        return String.format("update %s set (%s) values (%s) where id = ?", tableName, columnString, valuePlaceholders);
    }

    private String insertSQLBuilder() {
        String columnString = String.join(", ", columns);
        String valuePlaceholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        String[] nonIdCols = Arrays.copyOfRange(columns, 1, columns.length);
        String updateString = String.join(", ", Arrays.stream(nonIdCols).map(col -> col + " = ?").toArray(String[]::new));
        return String.format("insert into %s (%s) values (%s) ON CONFLICT (id) DO UPDATE SET %s;", tableName, columnString, valuePlaceholders, updateString);
    }

    private boolean isTableExists(Connection connection) throws SQLException {
        String sql = String.format("select count(*) from sqlite_master where type='table' and name='%s'", tableName);
        try (Statement statement = connection.createStatement();) {
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            return rs.getInt(1) != 0;
        }
    }

    public void createTableIfNotExists() throws IOException {
        try (Connection connection = DriverManager.getConnection(sqlitePrefix + dbSource);
             Statement statement = connection.createStatement();) {

            String sql = String.format("create table if not exists %s (" +
                        "id INTEGER PRIMARY KEY," +
                        "data BLOB," +
                        "ts TIMESTAMP" +
                    ")", tableName);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (insertStatement != null) {
            insertStatement.close();
        }
    }

    @Override
    public void write(Row feature, Context context) throws IOException, InterruptedException {
        String message = String.format("Sinking feature (%s) for (%s)", feature, id);
        try {
            Optional<Long> id = Optional.ofNullable((Long) feature.getField("id"));
            Optional<String> data = Optional.ofNullable((String) feature.getField("data"));
            Optional<Long> ts = Optional.ofNullable((Long) feature.getField("ts"));

            if (id.isEmpty() || data.isEmpty() || ts.isEmpty()) {
                throw new IOException("Feature is missing required fields");
            }

            insertStatement.setLong(1, id.get());
            insertStatement.setBytes(2, data.get().getBytes());
            insertStatement.setTimestamp(3, new Timestamp(ts.get()));
            insertStatement.setBytes(4, data.get().getBytes());
            insertStatement.setTimestamp(5, new Timestamp(ts.get()));
            insertStatement.addBatch();
            if (Instant.now().isAfter(time.plusSeconds(5))) {
                executeBatch();
                time = Instant.now();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
    private void executeBatch() throws IOException {
        String message = String.format("Executing batch for (%s)", id);
        try {
            insertStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        executeBatch();
    }
}
