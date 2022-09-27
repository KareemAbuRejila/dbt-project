package Consumer;

import DB.HiveConnection;
import Public.IKafkaConstants;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


public class SparkStream_Consumer {

    public static void main(String[] args) throws InterruptedException, IOException {
        SparkStream_Consumer c = new SparkStream_Consumer();
        c.display();



    }

    public void display() throws InterruptedException, IOException {

        //  delete temp files
        try {
            FileUtils.cleanDirectory(new File("/home/cloudera/Desktop/Project/BigDataProject/src/tmp/"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuilder lines1 = new StringBuilder();

        //Configure Spark to listen Data in topic test
        Collection<String> topics = Arrays.asList(IKafkaConstants.TOPIC_NAME);

        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkStream_Consumer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Start reading Data from Kafka and get DStream
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, ConsumerCreator.createSpark_Props()));


        //view Data
        JavaPairDStream<String, String> s = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        JavaDStream<String> lines = s.map(tuple2 -> tuple2._2());
        // Get the lines
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                List<String> list = stringJavaRDD.collect();

                for (String string : list) {
                    if (string != null) {
                        System.out.println("Data received: " + string);
                        //DBConnector.InsertIntoDB(string);

                        lines1.append(string);
                        lines1.append("\n");

                    }
                }


                //TODO:  Save Data
                FileWriter myWriterw = new FileWriter("/home/cloudera/Desktop/Project/BigDataProject/src/tmp/output.csv");
                myWriterw.write(String.valueOf(lines1));
                myWriterw.close();




            }



        });


        new  HiveConnection().InsertDataCOVID19("/home/cloudera/Desktop/Project/BigDataProject/src/tmp/output.csv",
                "/user/hive/COVID19","covid19" );

        jssc.start();
        jssc.awaitTermination();

        jssc.start();
        jssc.awaitTermination();






    }

}
