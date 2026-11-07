package dev.ratnesh.signalforge;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class LifecycleTest {
  @Test
  void deliveredNeverRegressesWhenSentArrivesLate() {
    assertEquals("DELIVERED", Event.state(4 | 2 | 1));
  }

  @Test
  void terminalConflictIsVisible() {
    assertEquals("CONFLICT", Event.state(4 | 8));
    assertEquals("CONFLICT", Event.state(8 | 16));
  }

  @Test
  void mergeIsCommutativeAndIdempotent() {
    for (int a = 0; a < 32; a++)
      for (int b = 0; b < 32; b++) {
        assertEquals(Event.state(a | b), Event.state(b | a));
        assertEquals(Event.state(a), Event.state(a | a));
      }
  }

  @Test
  void deterministicGeneratorHasReorderedAndDuplicateCallbacks() throws Exception {
    Event delivered = Event.parse(Generator.payload(0, "test", 0)),
        sent = Event.parse(Generator.payload(2, "test", 0)),
        duplicate = Event.parse(Generator.payload(3, "test", 0));
    assertTrue(delivered.time().isAfter(sent.time()));
    assertEquals(sent.eventId(), duplicate.eventId());
    assertEquals(sent.hash(), duplicate.hash());
  }

  @Test
  void invalidSchemasAreRejected() {
    assertThrows(Exception.class, () -> Event.parse("{}"));
    assertThrows(Exception.class, () -> Event.parse("not json"));
    assertThrows(
        Exception.class,
        () -> Event.parse(Generator.payload(0, "t", 0).replace("DELIVERED", "BOGUS")));
  }
}
