CREATE TEMP TABLE IF NOT EXISTS sf_stage (
 ordinal integer, topic text, partition_id integer, offset_id bigint, raw text, reason text,
 tenant text, provider text, event_id text, message_id text, attempt integer, kind text,
 event_time timestamptz, channel text, market text, hash text
) ON COMMIT DELETE ROWS;
