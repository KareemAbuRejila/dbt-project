package Producer;

import Public.IKafkaConstants;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class ProducerApp {
    public static void main(String[] args) throws IOException {
        //   runProducer();
        TimerTask tt = new TimerTask() {
            public void run() {


                URL url = null;
                try {
                    url = new URL("https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv");
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                URLConnection connection = null;
                try {
                    connection = url.openConnection();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                InputStreamReader input = null;
                try {
                    input = new InputStreamReader(connection.getInputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                BufferedReader buffer = null;
                //String line = "";

                try {
                    Producer<Long, String> producer = ProducerCreator.createProducer();


                    buffer = new BufferedReader(input);
                    CSVReader reader = new CSVReader(input, ',', '"', 1);
                    String[] nextLine;

                    while ((nextLine = reader.readNext()) != null) {
                        if (nextLine != null) {


                            //Verifying the read data here
                            String line = nextLine[0] + ";" + nextLine[1] + ";" + nextLine[2] + ";" + nextLine[3] + ";" + nextLine[4];
                            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, line);


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
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (buffer != null) {
                        try {
                            buffer.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }

        };

        Calendar today = Calendar.getInstance();
        today.set(Calendar.HOUR_OF_DAY, 2);
        today.set(Calendar.MINUTE, 0);
        today.set(Calendar.SECOND, 0);

// every night at 2am you run your task
        Timer timer = new Timer();
        timer.schedule(tt, today.getTime(), TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)); // period: 1 day
    }

    @SuppressWarnings("resource")
    static void runProducer() throws IOException {
        URL url = new URL("https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv");
        URLConnection connection = url.openConnection();

        InputStreamReader input = new InputStreamReader(connection.getInputStream());
        BufferedReader buffer = null;
        //String line = "";

        try {
            Producer<Long, String> producer = ProducerCreator.createProducer();


            buffer = new BufferedReader(input);
            CSVReader reader = new CSVReader(input, ',', '"', 1);
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine != null) {


                    //Verifying the read data here
                    String line = nextLine[0] + ";" + nextLine[1] + ";" + nextLine[2] + ";" + nextLine[3] + ";" + nextLine[4];
                    ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, line);


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
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (buffer != null) {
                try {
                    buffer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
