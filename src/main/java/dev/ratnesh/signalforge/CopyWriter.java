package dev.ratnesh.signalforge;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Base64;
import java.util.List;
import org.postgresql.PGConnection;

/** COPY and merge use the caller's transaction. No row-at-a-time JDBC calls or internal commits. */
public final class CopyWriter {
  public record SourceRecord(String topic, int partition, long offset, String raw) {}

  private final Connection db;
  private static final String STAGING = resource("/staging.sql"),
      MERGE = resource("/merge-stage.sql");

  public CopyWriter(Connection db) {
    this.db = db;
  }

  public void write(List<SourceRecord> records) throws Exception {
    if (db.getAutoCommit())
      throw new IllegalStateException("COPY merge requires an explicit transaction");
    if (records.isEmpty()) return;
    if (records.size() > 1000) throw new IllegalArgumentException("At most 1000 records per batch");
    try (var s = db.createStatement()) {
      s.execute(STAGING);
      s.execute("TRUNCATE sf_stage");
    }
    StringBuilder csv = new StringBuilder();
    for (int ordinal = 0; ordinal < records.size(); ordinal++) {
      var r = records.get(ordinal);
      Event e = null;
      String reason = null, raw = r.raw() == null ? "null" : r.raw();
      try {
        e = Event.parse(raw);
      } catch (Exception invalid) {
        reason = "SCHEMA: " + invalid.getClass().getSimpleName();
      }
      if (raw.indexOf('\0') >= 0) {
        reason = "SCHEMA_NUL_BASE64";
        e = null;
        raw = "base64:" + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
      }
      Object[] values = {
        ordinal,
        r.topic(),
        r.partition(),
        r.offset(),
        raw,
        reason,
        e == null ? null : e.tenant(),
        e == null ? null : e.provider(),
        e == null ? null : e.eventId(),
        e == null ? null : e.messageId(),
        e == null ? null : e.attempt(),
        e == null ? null : e.kind(),
        e == null ? null : e.time(),
        e == null ? null : e.channel(),
        e == null ? null : e.market(),
        e == null ? null : e.hash()
      };
      for (int i = 0; i < values.length; i++) {
        if (i > 0) csv.append(',');
        if (values[i] != null)
          csv.append('"').append(values[i].toString().replace("\"", "\"\"")).append('"');
      }
      csv.append('\n');
    }
    db.unwrap(PGConnection.class)
        .getCopyAPI()
        .copyIn("COPY sf_stage FROM STDIN WITH (FORMAT csv)", new StringReader(csv.toString()));
    try (var s = db.createStatement()) {
      s.execute(MERGE);
      while (s.getMoreResults() || s.getUpdateCount() != -1) {
        /* consume all results */
      }
    }
  }

  private static String resource(String path) {
    try (var in = CopyWriter.class.getResourceAsStream(path)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }
}
