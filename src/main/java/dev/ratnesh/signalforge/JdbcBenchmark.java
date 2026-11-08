package dev.ratnesh.signalforge;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;

/** Isolated sink benchmark. This intentionally does not claim Kafka/Spark/Parquet throughput. */
public class JdbcBenchmark {
 public static void main(String[] args)throws Exception{
  Path output=Path.of(Store.env("REPORT_DIR","benchmarks/results"));Files.createDirectories(output);
  int events=Integer.parseInt(Store.env("EVENTS","20000")),chunk=250;
  if(events<4 || events>1000000 || events%4!=0)throw new IllegalArgumentException("EVENTS must be divisible by four and <= 1M");
  Method bulk=null;Constructor<?> record=null;
  try{bulk=Store.class.getMethod("ingestBatch",List.class);record=Class.forName("dev.ratnesh.signalforge.CopyWriter$SourceRecord").getConstructor(String.class,int.class,long.class,String.class);}catch(ClassNotFoundException|NoSuchMethodException legacy){}
  try(Store store=Store.environment()){
   store.migrate();store.db.setAutoCommit(false);
   run(store,"warmup-"+UUID.randomUUID(),1000,chunk,bulk,record);
   for(int trial=1;trial<=3;trial++){
    String tenant="bench-"+UUID.randomUUID();long start=System.nanoTime();run(store,tenant,events,chunk,bulk,record);double seconds=(System.nanoTime()-start)/1e9;
    long unique=0,lifecycles=0;try(var s=store.db.prepareStatement("SELECT count(*) FROM event WHERE tenant=?")){s.setString(1,tenant);try(var r=s.executeQuery()){r.next();unique=r.getLong(1);}}
    try(var s=store.db.prepareStatement("SELECT count(*) FROM lifecycle WHERE tenant=? AND status='DELIVERED'")){s.setString(1,tenant);try(var r=s.executeQuery()){r.next();lifecycles=r.getLong(1);}}
    boolean correct=unique==events/4*3L && lifecycles==events/4;
    var result=new LinkedHashMap<String,Object>();result.put("trial",trial);result.put("mode",bulk==null?"row":"copy");result.put("commit",Store.env("BENCHMARK_REVISION","unknown"));result.put("timestamp",java.time.Instant.now().toString());result.put("rawEvents",events);result.put("canonicalEvents",unique);result.put("deliveredLifecycles",lifecycles);result.put("seconds",seconds);result.put("rawEventsPerSecond",events/seconds);result.put("canonicalEventsPerSecond",unique/seconds);result.put("batchSize",chunk);result.put("correct",correct);result.put("scope","single JDBC writer including JSON parsing, excluding Kafka Spark and Parquet");result.put("java",System.getProperty("java.version"));result.put("availableProcessors",Runtime.getRuntime().availableProcessors());
    String json=Event.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result);Files.writeString(output.resolve("run-"+trial+".json"),json);System.out.println(json);if(!correct)throw new IllegalStateException("sink oracle mismatch");
   }
  }
 }
 static void run(Store store,String tenant,int events,int chunk,Method bulk,Constructor<?> record)throws Exception{
  for(int start=0;start<events;start+=chunk){var rows=new ArrayList<Object>();for(int i=start;i<Math.min(events,start+chunk);i++){String raw=Generator.payload(i,tenant,0);if(bulk==null)store.ingest(tenant,0,i,raw);else rows.add(record.newInstance(tenant,0,(long)i,raw));}if(bulk!=null)bulk.invoke(store,rows);store.db.commit();}
 }
}
