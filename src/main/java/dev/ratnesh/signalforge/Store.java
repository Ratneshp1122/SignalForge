package dev.ratnesh.signalforge;
import java.sql.*;
import java.nio.charset.StandardCharsets;
public class Store implements AutoCloseable {
 public final Connection db;
 public Store(String url,String user,String password)throws Exception{db=DriverManager.getConnection(url,user,password);}
 public static Store environment()throws Exception{return new Store(env("DB_URL","jdbc:postgresql://localhost:5432/signalforge"),env("DB_USER","signalforge"),env("DB_PASSWORD","local-development-only"));}
 public static String env(String key,String fallback){return System.getenv().getOrDefault(key,fallback);}
 public void migrate()throws Exception{try(var in=Store.class.getResourceAsStream("/schema.sql");var s=db.createStatement()){s.execute(new String(in.readAllBytes(),StandardCharsets.UTF_8));}}
 public int execute(String sql,Object...args)throws SQLException{try(var s=db.prepareStatement(sql)){for(int i=0;i<args.length;i++)s.setObject(i+1,args[i]);return s.executeUpdate();}}
 public void ingest(String topic,int partition,long offset,String raw)throws Exception{
  Event event;
  try{event=Event.parse(raw);}catch(Exception invalid){quarantine(topic,partition,offset,"SCHEMA: "+invalid.getClass().getSimpleName(),raw);progress(topic,partition,offset);return;}
  Event e=event;
  // Serialize writes for one message attempt, including metadata conflict checks.
  try(var lock=db.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?,0))")){lock.setString(1,e.tenant()+"/"+e.provider()+"/"+e.messageId()+"/"+e.attempt());lock.execute();}
  try(var check=db.prepareStatement("SELECT channel,market FROM lifecycle WHERE tenant=? AND provider=? AND message_id=? AND attempt=?")){check.setString(1,e.tenant());check.setString(2,e.provider());check.setString(3,e.messageId());check.setInt(4,e.attempt());try(var r=check.executeQuery()){if(r.next()&&(!r.getString(1).equals(e.channel())||!r.getString(2).equals(e.market()))){quarantine(topic,partition,offset,"LIFECYCLE_METADATA_CONFLICT",raw);progress(topic,partition,offset);return;}}}
  int added=execute("INSERT INTO event(tenant,provider,event_id,message_id,attempt,kind,event_time,channel,market,hash,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",e.tenant(),e.provider(),e.eventId(),e.messageId(),e.attempt(),e.kind(),Timestamp.from(e.time()),e.channel(),e.market(),e.hash(),e.payload());
  if(added==0){try(var q=db.prepareStatement("SELECT hash FROM event WHERE tenant=? AND provider=? AND event_id=?")){q.setString(1,e.tenant());q.setString(2,e.provider());q.setString(3,e.eventId());try(var r=q.executeQuery()){r.next();if(!r.getString(1).equals(e.hash()))quarantine(topic,partition,offset,"EVENT_ID_CONFLICT",raw);}}}
  else {
   execute("INSERT INTO lifecycle VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (tenant,provider,message_id,attempt) DO UPDATE SET first_seen=least(lifecycle.first_seen,excluded.first_seen),last_seen=greatest(lifecycle.last_seen,excluded.last_seen),evidence=lifecycle.evidence|excluded.evidence",e.tenant(),e.provider(),e.messageId(),e.attempt(),e.channel(),e.market(),Timestamp.from(e.time()),Timestamp.from(e.time()),e.bit(),Event.state(e.bit()));
   execute("UPDATE lifecycle SET status=CASE WHEN ((evidence&4)>0)::int+((evidence&8)>0)::int+((evidence&16)>0)::int>1 THEN 'CONFLICT' WHEN (evidence&4)>0 THEN 'DELIVERED' WHEN (evidence&8)>0 THEN 'FAILED' WHEN (evidence&16)>0 THEN 'EXPIRED' WHEN (evidence&2)>0 THEN 'SENT' ELSE 'QUEUED' END WHERE tenant=? AND provider=? AND message_id=? AND attempt=?",e.tenant(),e.provider(),e.messageId(),e.attempt());
  }
  progress(topic,partition,offset);
 }
 void progress(String topic,int partition,long offset)throws SQLException{execute("INSERT INTO source_progress(topic,partition_id,offset_id) VALUES (?,?,?) ON CONFLICT(topic,partition_id) DO UPDATE SET offset_id=greatest(source_progress.offset_id,excluded.offset_id),updated_at=now()",topic,partition,offset);}
 void quarantine(String topic,int partition,long offset,String reason,String raw)throws SQLException{execute("INSERT INTO quarantine VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",topic,partition,offset,reason,raw==null?"null":raw);}
 @Override public void close()throws SQLException{db.close();}
}
