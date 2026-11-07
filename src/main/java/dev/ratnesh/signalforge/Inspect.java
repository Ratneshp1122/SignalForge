package dev.ratnesh.signalforge;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
public class Inspect {
 public static void main(String[] args)throws Exception{try(Store s=Store.environment()){
  for(String table:List.of("event","lifecycle","quarantine","archive_batch"))try(var q=s.db.createStatement();var r=q.executeQuery("SELECT count(*) FROM "+table)){r.next();System.out.println(table+"="+r.getLong(1));}
  try(var q=s.db.createStatement();var r=q.executeQuery("SELECT * FROM delivery_analytics ORDER BY window_start DESC LIMIT 20")){while(r.next())System.out.println("window="+r.getObject("window_start")+" channel="+r.getString("channel")+" market="+r.getString("market")+" delivered="+r.getLong("delivered"));}
  try(var admin=Admin.create(Map.of("bootstrap.servers",Store.env("KAFKA_BOOTSTRAP","localhost:9092")))){
   var info=admin.describeTopics(List.of("signalforge.raw.v1")).allTopicNames().get().get("signalforge.raw.v1");Map<TopicPartition,OffsetSpec> request=new HashMap<>();for(var p:info.partitions())request.put(new TopicPartition(info.name(),p.partition()),OffsetSpec.latest());var ends=admin.listOffsets(request).all().get();
   for(var entry:ends.entrySet()){long committed=-1;try(var q=s.db.prepareStatement("SELECT offset_id FROM source_progress WHERE topic=? AND partition_id=?")){q.setString(1,entry.getKey().topic());q.setInt(2,entry.getKey().partition());try(var r=q.executeQuery()){if(r.next())committed=r.getLong(1);}}System.out.println("partition="+entry.getKey().partition()+" lag="+Math.max(0,entry.getValue().offset()-committed-1));}
  }
 }}
}
