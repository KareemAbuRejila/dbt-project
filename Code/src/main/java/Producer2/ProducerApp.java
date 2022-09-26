package Producer2;

import Public.IKafkaConstants;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;


public class ProducerApp {
    public static void main(String[] args) throws IOException {
        runProducer();

    }

    @SuppressWarnings("resource")
    static void runProducer() throws IOException {

        try {
            Producer<Long, String> producer = ProducerCreator.createProducer();

            FileReader file = new FileReader("/home/cloudera/Desktop/Project/BigDataProject/src/LocalDataSource/COVID19_open_line_list.csv");
            CSVReader reader = new CSVReader(file, ',', '"', 1);
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine != null) {
                    //ID,age,sex,city,province,country, latitude,longitude,date_admission_hospital,date_confirmation,lives_in_Wuhan

                    String line = nextLine[0] + ";" + nextLine[1] + ";" + nextLine[2] + ";" + nextLine[3] + ";" + nextLine[4]+ ";" + nextLine[5]
                            + ";" + nextLine[7]  + ";" + nextLine[8] + ";" + nextLine[11] + ";" + nextLine[12] + ";" + nextLine[14] ;
                    //String line = String.join(";", nextLine);
                    ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME2, line);


                    try {
                        RecordMetadata metadata = producer.send(record).get();
                        System.out.println("Record sent with key " + line + " to partition " + metadata.partition() + " with offset " + metadata.offset());

                    } catch (ExecutionException e) {
                        System.out.println("Error in sending record");
                        System.out.println(e);
                    } catch (InterruptedException e) {
                        System.out.println("Error in sending record");
                        System.out.println(e);
                    }
                }
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }
}
