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
 import java.nio.ByteBuffer;
 import java.util.Map;
 
 public class CoustumInp
 {
  int balls;
  String Team1;
  String Team2;
  public CoustumIp(int b,String t1,String t2)
  {
   this.balls=b;
   this.Team1=t1;
   this.Team2=t2;
   }
  public int retPredscore()
  {
   return Predscore;
   }
  public int retballs()
  {
   return balls;
   }
  public String retTeam1()
  {
   return Team1;
   }
   public String retTeam2()
  {
   return Team2;
   }
  }
   
public class CoustomDeserializer implements Deserializer<CoustumIp>
{
  @Override
    public void configure(Map<String, ?> configs, boolean isKey) 
    {}
    public CoustumIp deserialize(String topicname,byte[] data)
    {
     ByteBuffer buf = ByteBuffer.wrap(data);
     int Balls = buf.getInt();
     int LenOfEncodedTeam1=buf.getInt();
     byte[] t1bytes=newe byte[LenOfEncodedTeam1]
     buf.get(t1bytes);
     String DTeam1=new String(t1bytes,"UTF8")
     int LenOfEncodedTeam2=buf.getInt();
     byte[] t2bytes=newe byte[LenOfEncodedTeam2]
     buf.get(t2bytes);
     String DTeam2=new String(t2bytes,"UTF8");
     return new CoustumIp(Balls,DTeam1,DTeam2);
     

     }
    
    @Override
    public void close() 
    {}
 }


public class CricConsumer
{

  public static void main(String[] args) throws Exception
  {
     Properties props = new Properties();
     props.put("bootstrap.servers", "localhost:9092,localhost:9093");
     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
     props.put("value.deserializer", "CoustomDeserializer");
     KafkaConsumer<String,CoustumIP> consumer = new KafkaConsumer<>(props);
     consumer.subscribe(Arrays.asList("Cricket_match"));
     int counter=0;
     List<String> listp = new ArrayList<String>();
     while (true)
     {
       ConsumerRecords<String, CoustumIp> records = consumer.poll(100);
       for (ConsumerRecord<String,CoustumIp> record : records)
       {

          try
            {
              FileWriter filew=new FileWriter("plist.txt");
              BufferedWriter bw =new BufferedWriter (filew);
              PrintWriter pw=new PrintWriter(bw);
        
              if(counter<11)
               {
                 listp.add(record.key()+" "+record.value().retballs()+" "+record.value().retTeam1()+" "+record.value().retTeam2());
               }
              else
               {
                 listp.remove(0);
                 listp.add(record.key()+" "+record.value().retballs()+" "+record.value().retTeam1()+" "+record.value().retTeam2());
                }
              if(counter%11==0)
                {
                  for(String rec:listp)
                     {
                       pw.println(rec);
                      } 
                 }
              pw.flush();
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

