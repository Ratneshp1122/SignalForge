CREATE TABLE IF NOT EXISTS archive_batch(id uuid PRIMARY KEY,state text NOT NULL DEFAULT 'PENDING',created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS event(
 tenant text NOT NULL,provider text NOT NULL,event_id text NOT NULL,message_id text NOT NULL,attempt int NOT NULL,
 kind text NOT NULL,event_time timestamptz NOT NULL,channel text NOT NULL,market text NOT NULL,hash text NOT NULL,payload text NOT NULL,
 archive_id uuid REFERENCES archive_batch,PRIMARY KEY(tenant,provider,event_id));
CREATE INDEX IF NOT EXISTS event_unarchived ON event(event_time) WHERE archive_id IS NULL;
CREATE TABLE IF NOT EXISTS lifecycle(tenant text,provider text,message_id text,attempt int,channel text NOT NULL,market text NOT NULL,first_seen timestamptz NOT NULL,last_seen timestamptz NOT NULL,evidence int NOT NULL,status text NOT NULL,PRIMARY KEY(tenant,provider,message_id,attempt));
CREATE INDEX IF NOT EXISTS lifecycle_analytics ON lifecycle(first_seen,channel,market);
CREATE TABLE IF NOT EXISTS quarantine(topic text,partition_id int,offset_id bigint,reason text NOT NULL,raw text NOT NULL,PRIMARY KEY(topic,partition_id,offset_id));
CREATE TABLE IF NOT EXISTS source_progress(topic text,partition_id int,offset_id bigint NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(topic,partition_id));
CREATE OR REPLACE VIEW delivery_analytics AS SELECT tenant,date_trunc('minute',first_seen) AS window_start,channel,market,count(*) AS attempts,count(*) FILTER(WHERE status='DELIVERED') AS delivered,count(*) FILTER(WHERE status='FAILED') AS failed,count(*) FILTER(WHERE status='CONFLICT') AS conflicts FROM lifecycle GROUP BY tenant,date_trunc('minute',first_seen),channel,market;
