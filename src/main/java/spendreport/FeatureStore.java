package spendreport;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class FeatureStore implements SinkWriter<Feature> {
    private final Connection connection;
    private final PreparedStatement preparedStatement;
    private final String tableName;
    private final String dbSource;
    private final String sqlitePrefix = "jdbc:sqlite:";
    private final String[] columns = {"id", "data", "ts", "idempotence_key"};
    private final String[] dTypes = {"integer", "blob", "timestamp", "text"};
    private transient Instant time = Instant.now();

    private final UUID id = UUID.randomUUID();

    public FeatureStore(String tableName, String dbSource) throws IOException {
        this.tableName = tableName;
        this.dbSource = dbSource;
        try {
            this.createTableIfNotExists();
            this.connection = DriverManager.getConnection(sqlitePrefix + dbSource);
            this.connection.setAutoCommit(false);
            this.preparedStatement = connection.prepareStatement(sqlBuilder());
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private String sqlBuilder() {
        String columnString = String.join(", ", columns);
        String valuePlaceholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        return String.format("insert into %s (%s) values (%s)", tableName, columnString, valuePlaceholders);
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
            StringBuilder schemaString = new StringBuilder();
            String delimiter;
            for (int i = 0; i < columns.length; i++) {
                delimiter = i < columns.length - 1 ? ", " : "";
                schemaString.append(String.format("%s %s%s", columns[i], dTypes[i], delimiter));
            }
            String sql = String.format("create table if not exists %s (%s)", tableName, schemaString);
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
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

    @Override
    public void write(Feature feature, Context context) throws IOException, InterruptedException {
        String message = String.format("Sinking feature (%s) for (%s)", feature, id);
        System.out.println(message);
        try {
            preparedStatement.setLong(1, feature.getId());
            preparedStatement.setBytes(2, feature.getData());
            preparedStatement.setTimestamp(3, new Timestamp(feature.getTs()));
            preparedStatement.setString(4, feature.getKey().toString());
            preparedStatement.addBatch();
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
        System.out.println(message);
        try {
            preparedStatement.executeBatch();
            System.out.println("Executing batch");
            connection.commit();
            System.out.println("Committing batch");
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        executeBatch();
    }
}
