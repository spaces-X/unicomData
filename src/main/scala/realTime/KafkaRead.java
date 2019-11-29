package realTime;

//public class KafkaRead {
//}

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaRead {

    public static void main(String[] args) {

//        String dzm="/usr/tools/xw/outdzm.txt";
//        String nl="/usr/tools/xw/outnl.txt";
//        int countDzm=0,countNl=0;
//
//        File dzmFile=new File(dzm);
//        File nlFile=new File(nl);
//
//        HashMap<String,Integer> dzmMap=new HashMap<String,Integer>();
//        HashMap<String,Integer> nlMap=new HashMap<String,Integer>();
//
//        BufferedReader bfdzm=null,bfnl=null;
//        try {
//            bfdzm=new BufferedReader(new FileReader(dzmFile));
//            bfnl=new BufferedReader(new FileReader(nlFile));
//
//            String lineDzm="";
//            String lineNl="";
//
//            while((lineDzm=bfdzm.readLine())!=null)
//            {
//                dzmMap.put(lineDzm,1);
//            }
//
//            while ((lineNl=bfnl.readLine())!=null)
//            {
//                nlMap.put(lineNl,1);
//            }
//            bfdzm.close();
//            bfnl.close();
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        Properties props = new Properties();
        props.put("bootstrap.servers", "132.90.117.70:9092,132.90.117.71:9092,132.90.117.73:9092,132.90.117.74:9092,132.90.117.75:9092");
        props.put("group.id", "signalling234g");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        System.setProperty("java.security.auth.login.config", "/usr/tools/xw/kafka_client_jaas1.conf");
        props.put("security.protocol","SASL_PLAINTEXT");
        props.put("sasl.mechanism","PLAIN");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("signalling_234g_mask");
        List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<TopicPartition,Long>();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String t="2019-11-29 12:27:00";
        Date now = new Date();
        long nowTime = now.getTime();
        nowTime -= 3*60*1000;
        System.out.println("当前时间: " + df.format(now));
        long fetchDataTime = 0;  // 计算30分钟之前的时间戳
        try {
            fetchDataTime = df.parse(t).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        for(PartitionInfo partitionInfo : partitionInfos) {
            topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), nowTime);
        }

        consumer.assign(topicPartitions);

        // 获取每个partition一个小时之前的偏移量
        Map<TopicPartition, OffsetAndTimestamp> map = consumer.offsetsForTimes(timestampsToSearch);

        OffsetAndTimestamp offsetTimestamp = null;
        System.out.println("开始设置各分区初始偏移量...");
        for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
            // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
            offsetTimestamp = entry.getValue();
            if(offsetTimestamp != null) {
                int partition = entry.getKey().partition();
                long timestamp = offsetTimestamp.timestamp();
                long offset = offsetTimestamp.offset();
                System.out.println("partition = " + partition +
                        ", time = " + df.format(new Date(timestamp))+
                        ", offset = " + offset);
                // 设置读取消息的偏移量
                consumer.seek(entry.getKey(), offset);
            }
        }
        System.out.println("设置各分区初始偏移量结束...");


        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Starting exit...");
                consumer.wakeup();
                try {
                    //等待主线程退出
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        String value="";
        String ci="";
        int countAll=0;
        String first="";
        Boolean f=true;


        long start=System.currentTimeMillis();
        try
        {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                    {
                        System.out.println(record.value());
//                        if(f)
//                        {
//                            first=record.value();
//                            f=false;
//                        }
//                        countAll++;
//                        value=record.value();
//                        ci=value.split("\\|")[8];
//
//                        if(dzmMap.containsKey(ci))
//                            countDzm++;
//                        if (nlMap.containsKey(ci))
//                            countNl++;
                    }
                }
            }
            catch (WakeupException e){

            }
            finally {
                long end=System.currentTimeMillis();
                consumer.close();
                System.out.println("done.======Consumer is closed");
                System.out.println(first);
                System.out.println(" time: " + (end - start));
//                System.out.println("dzm   "+countDzm+"    nl    "+countNl+"    all    "+countAll+"    time     "+(end-start));
                System.out.println(value);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}

