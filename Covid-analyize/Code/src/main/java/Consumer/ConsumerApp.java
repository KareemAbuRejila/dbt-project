package Consumer;

import DB.HiveConnection;
import Public.IKafkaConstants;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import Consumer.ConsumerCreator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class ConsumerApp {
    public static void main(String[] args) throws IOException {

        runConsumer();
    }

    static void runConsumer() throws IOException {

        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        StringBuilder lines = new StringBuilder();


        //  delete temp files
        try {
            FileUtils.cleanDirectory(new File("/home/cloudera/Desktop/Project/BigDataProject/src/tmp/"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        int noMessageFound = 0;
        while (true) {

            ConsumerRecords<Long, String> consumerRecords = consumer.poll(10);
            // 1000 is the time in milliseconds consumer will wait if no record
            // is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
                    // If no message found count is reached to threshold exit loop.
                    System.out.println("consumerRecords.count()  " + consumerRecords.count());
                    break;
                } else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record value " + record.value() + "  Record partition " + record.partition() + " Record offset " + record.offset());

                //Date,Country,Confirmed,Recovered,Deaths
                try {
                    lines.append(record.value());
                    lines.append("\n");


                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("append error  " + e.getMessage());

                }
            });

            consumer.commitAsync();
            //TODO:  Save Data
            FileWriter myWriterw = new FileWriter("/home/cloudera/Desktop/Project/BigDataProject/src/tmp/output.csv");
            myWriterw.write(String.valueOf(lines));
            myWriterw.close();


        }
        //TODO:  Spark insert Data in Hive
        new  HiveConnection().InsertDataCOVID19("/home/cloudera/Desktop/Project/BigDataProject/src/tmp/output.csv",
                "/user/hive/COVID19","covid19");
        consumer.close();


    }


}