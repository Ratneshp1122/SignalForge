package dev.ratnesh.signalforge;

import com.fasterxml.jackson.databind.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

public record Event(
    String tenant,
    String provider,
    String eventId,
    String messageId,
    int attempt,
    String kind,
    Instant time,
    String channel,
    String market,
    String payload,
    String hash) {
  static final ObjectMapper JSON = new ObjectMapper();

  public static Event parse(String raw) throws Exception {
    if (raw == null || raw.length() > 65536) throw new IllegalArgumentException("payload size");
    JsonNode n = JSON.readTree(raw);
    if (!n.isObject()
        || !n.path("schemaVersion").isIntegralNumber()
        || n.path("schemaVersion").asInt() != 1)
      throw new IllegalArgumentException("schema version");
    String tenant = text(n, "tenant"),
        provider = text(n, "provider"),
        id = text(n, "eventId"),
        message = text(n, "messageId"),
        kind = text(n, "kind"),
        channel = text(n, "channel"),
        market = text(n, "market");
    if (!n.path("attempt").isIntegralNumber()
        || !n.path("attempt").canConvertToInt()
        || n.path("attempt").asInt() < 1) throw new IllegalArgumentException("attempt");
    int attempt = n.path("attempt").asInt();
    Instant time = Instant.parse(text(n, "eventTime"));
    if (!Set.of("QUEUED", "SENT", "DELIVERED", "FAILED", "EXPIRED").contains(kind))
      throw new IllegalArgumentException("kind");
    if (!Set.of("SMS", "EMAIL", "WHATSAPP").contains(channel))
      throw new IllegalArgumentException("channel");
    // Canonical fields only: whitespace/key ordering and opaque padding do not change identity.
    String canonical =
        JSON.writeValueAsString(
            List.of(
                tenant, provider, id, message, attempt, kind, time.toString(), channel, market));
    return new Event(
        tenant,
        provider,
        id,
        message,
        attempt,
        kind,
        time,
        channel,
        market,
        raw,
        HexFormat.of()
            .formatHex(
                MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8))));
  }

  static String text(JsonNode n, String key) {
    if (!n.path(key).isTextual()
        || n.path(key).asText().isBlank()
        || n.path(key).asText().length() > 128) throw new IllegalArgumentException(key);
    return n.path(key).asText();
  }

  public int bit() {
    return switch (kind) {
      case "QUEUED" -> 1;
      case "SENT" -> 2;
      case "DELIVERED" -> 4;
      case "FAILED" -> 8;
      case "EXPIRED" -> 16;
      default -> throw new IllegalArgumentException();
    };
  }

  public static String state(int bits) {
    int terminal = bits & 28;
    if (Integer.bitCount(terminal) > 1) return "CONFLICT";
    if ((bits & 4) > 0) return "DELIVERED";
    if ((bits & 8) > 0) return "FAILED";
    if ((bits & 16) > 0) return "EXPIRED";
    return (bits & 2) > 0 ? "SENT" : "QUEUED";
  }
}
