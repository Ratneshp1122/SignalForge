package dev.ratnesh.signalforge;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;

public class Store implements AutoCloseable {
  public final Connection db;

  public Store(String url, String user, String password) throws Exception {
    db = DriverManager.getConnection(url, user, password);
  }

  public static Store environment() throws Exception {
    return new Store(
        env("DB_URL", "jdbc:postgresql://localhost:5432/signalforge"),
        env("DB_USER", "signalforge"),
        env("DB_PASSWORD", "local-development-only"));
  }

  public static String env(String key, String fallback) {
    return System.getenv().getOrDefault(key, fallback);
  }

  public void migrate() throws Exception {
    try (var in = Store.class.getResourceAsStream("/schema.sql");
        var s = db.createStatement()) {
      s.execute(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  public int execute(String sql, Object... args) throws SQLException {
    try (var s = db.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) s.setObject(i + 1, args[i]);
      return s.executeUpdate();
    }
  }

  public void ingest(String topic, int partition, long offset, String raw) throws Exception {
    ingestBatch(List.of(new CopyWriter.SourceRecord(topic, partition, offset, raw)));
  }

  public void ingestBatch(List<CopyWriter.SourceRecord> rows) throws Exception {
    new CopyWriter(db).write(rows);
  }

  @Override
  public void close() throws SQLException {
    db.close();
  }
}
