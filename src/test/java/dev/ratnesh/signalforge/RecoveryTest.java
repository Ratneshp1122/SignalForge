package dev.ratnesh.signalforge;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.containers.PostgreSQLContainer;
import static org.junit.jupiter.api.Assertions.*;
@Tag("integration") @Testcontainers
class RecoveryTest {
 @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:17.6-alpine");
 Store open()throws Exception{Store s=new Store(postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword());s.migrate();s.db.setAutoCommit(false);return s;}
 long count(Store s,String table)throws Exception{try(var q=s.db.createStatement();var r=q.executeQuery("SELECT count(*) FROM "+table)){r.next();return r.getLong(1);}}
 @Test void duplicateSuppressionAndReplayAfterCommit()throws Exception{String run=java.util.UUID.randomUUID().toString();try(Store s=open()){for(int i=0;i<4;i++)s.ingest("raw",0,i,Generator.payload(i,run,0));s.db.commit();for(int i=0;i<4;i++)s.ingest("raw",0,i,Generator.payload(i,run,0));s.db.commit();try(var q=s.db.prepareStatement("SELECT count(*) FROM event WHERE tenant=?")){q.setString(1,run);try(var r=q.executeQuery()){r.next();assertEquals(3,r.getLong(1));}}try(var q=s.db.prepareStatement("SELECT status FROM lifecycle WHERE tenant=?")){q.setString(1,run);try(var r=q.executeQuery()){r.next();assertEquals("DELIVERED",r.getString(1));}}}}
 @Test void rolledBackChunkIsReplayed()throws Exception{String run=java.util.UUID.randomUUID().toString();try(Store s=open()){long before=count(s,"event");s.ingest("rollback",0,0,Generator.payload(0,run,0));s.db.rollback();assertEquals(before,count(s,"event"));s.ingest("rollback",0,0,Generator.payload(0,run,0));s.db.commit();assertEquals(before+1,count(s,"event"));}}
 @Test void malformedAndIdentityConflictsAreQuarantined()throws Exception{String run=java.util.UUID.randomUUID().toString();try(Store s=open()){long before=count(s,"quarantine");String raw=Generator.payload(0,run,0);s.ingest(run,0,0,raw);s.ingest(run,0,1,raw.replace("\"kind\":\"DELIVERED\"","\"kind\":\"FAILED\""));s.ingest(run,0,2,"{broken");s.ingest(run,0,2,"{broken");s.db.commit();assertEquals(before+2,count(s,"quarantine"));}}
}
