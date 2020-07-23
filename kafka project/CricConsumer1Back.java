import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
 import java.io.PrintWriter;

public class CricConsumer1
{

  public static void main(String[] args) throws Exception
  {
     Properties props = new Properties();
     props.put("bootstrap.servers", "localhost:9092");
     props.put("group.id","gropname");
     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
     KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
     consumer.subscribe(Arrays.asList("Cricket_match"));
     int counter=0;
     List<String> listp = new ArrayList<String>();
     while (true)
     {
       ConsumerRecords<String,String> records = consumer.poll(100);
       for (ConsumerRecord<String,String> record:records)
       {

          try
            {
              FileWriter filew=new FileWriter("plist.txt",false);
              BufferedWriter bw =new BufferedWriter (filew);
              PrintWriter pw=new PrintWriter(bw);
        
              if(counter<11)
               {
                 listp.add(record.key()+" "+record.value());
                 counter=counter+1
               }
              else
               {
                 listp.remove(0);
                 listp.add(record.key()+" "+record.value());
                }
              if(counter%11==0)
                { pw.print("");
                  for(String rec:listp)
                     {
                       pw.println(rec);
                      } 
                 }
              pw.close();
              bw.close ();
              filew.close();

    }
  catch (IOException e)
    {
      continue;
    }
    


        
       }
     }

   }
}

