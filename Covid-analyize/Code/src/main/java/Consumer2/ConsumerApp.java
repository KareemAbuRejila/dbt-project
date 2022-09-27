package Consumer2;

import DB.COVID19Lines;
import DB.HBaseConnection;
import DB.HiveConnection;
import Public.IKafkaConstants;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ConsumerApp {
    public static void main(String[] args) throws IOException {

        runConsumer();
    }

    static void runConsumer() throws IOException {

        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        //StringBuilder lines = new StringBuilder();

        List<COVID19Lines> data = new ArrayList<COVID19Lines>();
        //  delete temp files
        try {
            FileUtils.cleanDirectory(new File("/home/cloudera/Desktop/Project/BigDataProject/src/tmp2/"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        int noMessageFound = 0;
        while (true) {

            ConsumerRecords<Long, String> consumerRecords;
            consumerRecords = consumer.poll(1000);
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

                try {
                    String str = record.value();
                    String[] line = str.split(";");
                    //ID,age,sex,city,province,country, latitude,longitude,date_admission_hospital,date_confirmation,lives_in_Wuhan

                    String lastlinepart = "";

                    String line1 = "";
                    String line2 = "";
                    String line3 = "";
                    String line4 = "";
                    String line5 = "";
                    String line6 = "";
                    String line7 = "";
                    String line8 = "";
                    String line9 = "";
                    String line10 = "";
                    String line11 = "";

                    try { line1 = line[0]; }catch (Exception e) {}
                    try { line2 = line[1];}catch (Exception e) {}
                    try { line3 = line[2];}catch (Exception e) {}
                    try { line4 = line[3];}catch (Exception e) {}
                    try { line5 = line[4];}catch (Exception e) {}
                    try { line6 = line[5];}catch (Exception e) {}
                    try { line7 = line[6];}catch (Exception e) {}
                    try { line8 = line[7];}catch (Exception e) {}
                    try { line9 = line[8];}catch (Exception e) {}
                    try { line10= line[9];}catch (Exception e) {}
                    try { line11= line[10];}catch (Exception e) {}


                    COVID19Lines c = new COVID19Lines(
                            line1,
                            line2,
                            line3,
                            line4,
                            line5,
                            line6,
                            line7,
                            line8,
                            line9,
                            line10,
                            line11);


                    if (line1.trim() != "" &&
                            line2.trim() != "" &&
                            line3.trim() != "" &&
                            line4.trim() != "" &&
                            line5.trim() != "" &&
                            line6.trim() != "" &&
                            line7.trim() != "" &&
                            line8.trim() != "" &&
                            line9.trim() != "" &&
                            line10.trim() != "" &&
                            line11.trim() != "") {

                        data.add(c);
                    }

                    //    HBaseConnection.InsertCOVID19LINES(c);

//                    lines.append(record.value());
//                    lines.append("\n");

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("append error  " + e.getMessage());
                }

            });

            consumer.commitAsync();


        }
        if (data.size() > 0) {
            // Save Data in HBase
            HBaseConnection.InsertCOVID19LINES(data);

            try {
                //TODO:  Save Data
                FileWriter myWriterw = new FileWriter("/home/cloudera/Desktop/Project/BigDataProject/src/tmp2/output.csv");

                // TODO: Read Data from HBase and Cleaning   if (age == null || sex == null || country == null) continue;
                myWriterw.write(String.valueOf(HBaseConnection.RetrieveCOVID19LINES()));
                myWriterw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            //TODO:  Spark insert Data in Hive
            new HiveConnection().InsertDataCOVID19("/home/cloudera/Desktop/Project/BigDataProject/src/tmp2/output.csv",
                    "/user/hive/COVID19LINES", "covid19LINES");

        }
        consumer.close();


    }


}