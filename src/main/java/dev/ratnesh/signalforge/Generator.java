package dev.ratnesh.signalforge;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.time.*;
import java.util.concurrent.atomic.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.LockSupport;
/** Deterministic reordered groups: DELIVERED arrives before SENT; every fourth callback repeats SENT. */
public class Generator {
 public static String payload(long i,String run,int padding)throws Exception{
  long message=i/4;int position=(int)(i%4);String kind=switch(position){case 0->"DELIVERED";case 1->"QUEUED";default->"SENT";};
  var n=new TreeMap<String,Object>();n.put("schemaVersion",1);n.put("tenant",run);n.put("provider","synthetic");n.put("eventId",message+"-"+kind);n.put("messageId","m-"+message);n.put("attempt",1);n.put("kind",kind);n.put("eventTime",Instant.parse("2026-01-01T00:00:00Z").plusMillis(message*10+(kind.equals("DELIVERED")?2000:kind.equals("SENT")?1000:0)).toString());n.put("channel",message%2==0?"SMS":"EMAIL");n.put("market",message%3==0?"IN":"US");n.put("padding","x".repeat(padding));return Event.JSON.writeValueAsString(n);
 }
 public static void main(String[] args)throws Exception{
  long count=Long.parseLong(Store.env("EVENTS","10000")),rate=Long.parseLong(Store.env("RATE","1000"));int padding=Integer.parseInt(Store.env("PADDING","0"));String run=Store.env("RUN_ID",UUID.randomUUID().toString());
  if(count<1||rate<1||padding<0||padding>60000)throw new IllegalArgumentException("invalid workload");
  Properties props=new Properties();props.put("bootstrap.servers",Store.env("KAFKA_BOOTSTRAP","localhost:9092"));props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");props.put("acks","all");props.put("enable.idempotence","true");props.put("linger.ms","5");props.put("batch.size","65536");props.put("compression.type","lz4");
  AtomicLong acknowledged=new AtomicLong(),errors=new AtomicLong();long bytes=0,start=System.nanoTime();
  try(var producer=new KafkaProducer<String,String>(props)){
   for(long i=0;i<count;i++){long due=start+(long)((double)i/rate*1_000_000_000d),wait=due-System.nanoTime();if(wait>0)LockSupport.parkNanos(wait);String raw=payload(i,run,padding);bytes+=raw.getBytes(StandardCharsets.UTF_8).length;producer.send(new ProducerRecord<>("signalforge.raw.v1",run+":"+(i/4),raw),(metadata,error)->{if(error==null)acknowledged.incrementAndGet();else errors.incrementAndGet();});}
   producer.flush();
  }
  System.out.println(Event.JSON.writeValueAsString(Map.of("runId",run,"requested",count,"acknowledged",acknowledged.get(),"errors",errors.get(),"payloadBytes",bytes,"seconds",(System.nanoTime()-start)/1e9,"targetRate",rate,"expectedUniqueEvents",(count/4)*3+Math.min(count%4,3),"expectedLifecycles",(count+3)/4)));
  if(errors.get()>0)throw new IllegalStateException("generator delivery errors");
 }
}
