package dev.ratnesh.signalforge;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;

@Tag("integration")
@Testcontainers
class CopyWriterTest {
  @Container static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:17.6-alpine");

  @BeforeAll
  static void schema() throws Exception {
    try (Store s = open()) {
      s.migrate();
    }
  }

  static Store open() throws Exception {
    return new Store(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
  }

  List<CopyWriter.SourceRecord> batch(String tenant, int count) throws Exception {
    var rows = new ArrayList<CopyWriter.SourceRecord>();
    for (int i = 0; i < count; i++)
      rows.add(new CopyWriter.SourceRecord(tenant, 0, i, Generator.payload(i, tenant, 0)));
    return rows;
  }

  long count(Store s, String tenant) throws Exception {
    try (var q = s.db.prepareStatement("SELECT count(*) FROM event WHERE tenant=?")) {
      q.setString(1, tenant);
      try (var r = q.executeQuery()) {
        r.next();
        return r.getLong(1);
      }
    }
  }

  @Test
  void bulkReplayAndRollbackPreserveCanonicalCounts() throws Exception {
    String t = UUID.randomUUID().toString();
    var rows = batch(t, 1000);
    try (Store s = open()) {
      s.db.setAutoCommit(false);
      s.ingestBatch(rows);
      s.db.rollback();
      assertEquals(0, count(s, t));
      s.ingestBatch(rows);
      s.db.commit();
      s.ingestBatch(rows);
      s.db.commit();
      assertEquals(750, count(s, t));
      try (var q = s.db.createStatement();
          var r =
              q.executeQuery(
                  "SELECT count(*) FROM lifecycle WHERE tenant='"
                      + t
                      + "' AND status='DELIVERED'")) {
        r.next();
        assertEquals(250, r.getLong(1));
      }
    }
  }

  @Test
  void conflictingVersionsInOneBatchAreAllQuarantined() throws Exception {
    String t = UUID.randomUUID().toString();
    String raw = Generator.payload(0, t, 0);
    var rows =
        List.of(
            new CopyWriter.SourceRecord(t, 0, 0, raw),
            new CopyWriter.SourceRecord(
                t, 0, 1, raw.replace("\"kind\":\"DELIVERED\"", "\"kind\":\"FAILED\"")));
    try (Store s = open()) {
      s.db.setAutoCommit(false);
      s.ingestBatch(rows);
      s.db.commit();
      assertEquals(0, count(s, t));
      try (var q = s.db.createStatement();
          var r = q.executeQuery("SELECT count(*) FROM quarantine WHERE topic='" + t + "'")) {
        r.next();
        assertEquals(2, r.getInt(1));
      }
    }
  }

  @Test
  void csvEscapesAndNulQuarantineDoNotAbortGoodRows() throws Exception {
    String t = UUID.randomUUID().toString();
    String raw =
        Generator.payload(0, t, 0)
            .replace("\"padding\":\"\"", "\"padding\":\"comma, quote\\\" newline\\n\"");
    try (Store s = open()) {
      s.db.setAutoCommit(false);
      s.ingestBatch(
          List.of(
              new CopyWriter.SourceRecord(t, 0, 0, raw),
              new CopyWriter.SourceRecord(t, 0, 1, "bad\0json")));
      s.db.commit();
      assertEquals(1, count(s, t));
    }
  }

  @Test
  void overlappingConcurrentBatchesCannotDoubleCount() throws Exception {
    String t = UUID.randomUUID().toString();
    var rows = batch(t, 400);
    var reversed = new ArrayList<>(rows);
    Collections.reverse(reversed);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      var start = new CountDownLatch(1);
      var futures = new ArrayList<Future<?>>();
      for (var input : List.of(rows, reversed))
        futures.add(
            pool.submit(
                () -> {
                  try (Store s = open()) {
                    s.db.setAutoCommit(false);
                    start.await();
                    s.ingestBatch(input);
                    s.db.commit();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }));
      start.countDown();
      for (var f : futures) f.get(30, TimeUnit.SECONDS);
      try (Store s = open()) {
        assertEquals(300, count(s, t));
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void newLifecycleMetadataConflictIsExplicit() throws Exception {
    String t = UUID.randomUUID().toString();
    String a = Generator.payload(0, t, 0),
        b = Generator.payload(1, t, 0).replace("\"market\":\"IN\"", "\"market\":\"US\"");
    try (Store s = open()) {
      s.db.setAutoCommit(false);
      s.ingestBatch(
          List.of(
              new CopyWriter.SourceRecord(t, 0, 0, a), new CopyWriter.SourceRecord(t, 0, 1, b)));
      s.db.commit();
      assertEquals(0, count(s, t));
    }
  }
}
