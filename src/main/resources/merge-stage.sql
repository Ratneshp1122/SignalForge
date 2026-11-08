-- Acquire both identity domains in one global lock order before reading canonical rows.
SELECT pg_advisory_xact_lock(lock_id) FROM (
 SELECT DISTINCT hashtextextended(jsonb_build_array('sf-event',tenant,provider,event_id)::text,0) lock_id FROM sf_stage WHERE reason IS NULL
 UNION
 SELECT DISTINCT hashtextextended(jsonb_build_array('sf-life',tenant,provider,message_id,attempt)::text,0) FROM sf_stage WHERE reason IS NULL
) locks ORDER BY lock_id;

UPDATE sf_stage s SET reason='EVENT_ID_CONFLICT' FROM event e
WHERE s.reason IS NULL AND (e.tenant,e.provider,e.event_id)=(s.tenant,s.provider,s.event_id) AND e.hash<>s.hash;

-- Quarantine all competing versions when the batch has no previously committed winner.
UPDATE sf_stage s SET reason='EVENT_ID_CONFLICT' FROM (
 SELECT tenant,provider,event_id FROM sf_stage WHERE reason IS NULL
 GROUP BY tenant,provider,event_id HAVING count(DISTINCT hash)>1
) c WHERE s.reason IS NULL AND (s.tenant,s.provider,s.event_id)=(c.tenant,c.provider,c.event_id);

UPDATE sf_stage s SET reason='LIFECYCLE_METADATA_CONFLICT' FROM lifecycle l
WHERE s.reason IS NULL AND (l.tenant,l.provider,l.message_id,l.attempt)=(s.tenant,s.provider,s.message_id,s.attempt)
AND (l.channel,l.market) IS DISTINCT FROM (s.channel,s.market);

UPDATE sf_stage s SET reason='LIFECYCLE_METADATA_CONFLICT' FROM (
 SELECT tenant,provider,message_id,attempt FROM sf_stage WHERE reason IS NULL
 GROUP BY tenant,provider,message_id,attempt HAVING count(DISTINCT (channel,market))>1
) c WHERE s.reason IS NULL AND (s.tenant,s.provider,s.message_id,s.attempt)=(c.tenant,c.provider,c.message_id,c.attempt);

WITH winners AS (
 SELECT DISTINCT ON (tenant,provider,event_id) * FROM sf_stage WHERE reason IS NULL ORDER BY tenant,provider,event_id,ordinal
), inserted AS (
 INSERT INTO event(tenant,provider,event_id,message_id,attempt,kind,event_time,channel,market,hash,payload)
 SELECT tenant,provider,event_id,message_id,attempt,kind,event_time,channel,market,hash,raw FROM winners
 ORDER BY tenant,provider,event_id ON CONFLICT DO NOTHING RETURNING *
), aggregate AS (
 SELECT tenant,provider,message_id,attempt,channel,market,min(event_time) first_seen,max(event_time) last_seen,
 bit_or(CASE kind WHEN 'QUEUED' THEN 1 WHEN 'SENT' THEN 2 WHEN 'DELIVERED' THEN 4 WHEN 'FAILED' THEN 8 ELSE 16 END) evidence
 FROM inserted GROUP BY tenant,provider,message_id,attempt,channel,market
)
INSERT INTO lifecycle SELECT tenant,provider,message_id,attempt,channel,market,first_seen,last_seen,evidence,sf_lifecycle_state(evidence)
FROM aggregate ORDER BY tenant,provider,message_id,attempt
ON CONFLICT(tenant,provider,message_id,attempt) DO UPDATE SET
 first_seen=least(lifecycle.first_seen,excluded.first_seen), last_seen=greatest(lifecycle.last_seen,excluded.last_seen),
 evidence=lifecycle.evidence|excluded.evidence, status=sf_lifecycle_state(lifecycle.evidence|excluded.evidence);

INSERT INTO quarantine(topic,partition_id,offset_id,reason,raw)
SELECT DISTINCT ON(topic,partition_id,offset_id) topic,partition_id,offset_id,reason,raw FROM sf_stage WHERE reason IS NOT NULL
ORDER BY topic,partition_id,offset_id,ordinal ON CONFLICT DO NOTHING;

INSERT INTO source_progress(topic,partition_id,offset_id)
SELECT topic,partition_id,max(offset_id) FROM sf_stage GROUP BY topic,partition_id ORDER BY topic,partition_id
ON CONFLICT(topic,partition_id) DO UPDATE SET offset_id=greatest(source_progress.offset_id,excluded.offset_id),updated_at=now();
