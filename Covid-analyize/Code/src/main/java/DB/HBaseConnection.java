package DB;

//Place this code inside Hbase connection

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


public class HBaseConnection {

    private static final String TABLE_NAME = "COVID19LINES";
    private static final String FAMILY_NAME = "f1";
    public static HTable hTable;

    public static void InsertCOVID19LINES(COVID19Lines t) throws IOException {


        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            // addFamily
            table.addFamily(new HColumnDescriptor(FAMILY_NAME));

            System.out.print("Creating table.... ");
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
            System.out.println(" Done!");

            // inistiating the table
            hTable = new HTable(config, TABLE_NAME);

            // put data to table
            System.out.print("Putting data into table.... ");


            Put p = new Put(Bytes.toBytes(t.getID()));
            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("age"), Bytes.toBytes(t.getAge()));
            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("sex"), Bytes.toBytes(t.getSex()));
            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("city"), Bytes.toBytes(t.getCity()));
            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("province"), Bytes.toBytes(t.getProvince()));
            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("country"), Bytes.toBytes(t.getCountry()));

            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("latitude"), Bytes.toBytes(t.getLatitude()));

            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("longitude"), Bytes.toBytes(t.getLongitude()));

            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("date_admission_hospital"), Bytes.toBytes(t.getDate_admission_hospital().toString()));

            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("date_confirmation"), Bytes.toBytes(t.getDate_confirmation().toString()));
            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("lives_in_Wuhan"), Bytes.toBytes(t.getLives_in_Wuhan()));
            p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("country"), Bytes.toBytes(t.getCountry()));
            hTable.put(p);


            // table.clone();
            connection.close();

            System.out.println(" Done!");


        }
    }



    public static void InsertCOVID19LINES(List<COVID19Lines> list) throws IOException {


        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            // addFamily
            table.addFamily(new HColumnDescriptor(FAMILY_NAME));

            System.out.print("Creating table.... ");
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
            System.out.println(" Done!");

            // inistiating the table
            hTable = new HTable(config, TABLE_NAME);

            // put data to table
            System.out.print("Putting data into table.... ");

            for (COVID19Lines t : list) {
                Put p = new Put(Bytes.toBytes(t.getID()));
                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("age"), Bytes.toBytes(t.getAge()));
                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("sex"), Bytes.toBytes(t.getSex()));
                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("city"), Bytes.toBytes(t.getCity()));
                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("province"), Bytes.toBytes(t.getProvince()));
                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("country"), Bytes.toBytes(t.getCountry()));

                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("latitude"), Bytes.toBytes(t.getLatitude()));

                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("longitude"), Bytes.toBytes(t.getLongitude()));

                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("date_admission_hospital"), Bytes.toBytes(t.getDate_admission_hospital().toString()));

                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("date_confirmation"), Bytes.toBytes(t.getDate_confirmation().toString()));
                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("lives_in_Wuhan"), Bytes.toBytes(t.getLives_in_Wuhan()));
                p.add(FAMILY_NAME.getBytes(), Bytes.toBytes("country"), Bytes.toBytes(t.getCountry()));
                hTable.put(p);
            }

            // table.clone();
            connection.close();

            System.out.println(" Done!");


        }
    }



    public static StringBuilder RetrieveCOVID19LINES() throws IOException {
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        // Instantiating HTable class

        StringBuilder lines = new StringBuilder();

        // 13479 rows number in csv file
        for (int i = 1; i <= 13479; i++) {
            // Instantiating Get class
            HTable table = new HTable(config, TABLE_NAME);
            Get g = new Get(Bytes.toBytes(i+""));

            // Reading the data
            Result result = table.get(g);

            // Reading values from Result class object
            String age = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("age")));
            String sex = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("sex")));
            String city = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("city")));
            String province = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("province")));
            String country = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("country")));

            String latitude = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("latitude")));

            String longitude = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("longitude")));

            String date_admission_hospital = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("date_admission_hospital")));

            String date_confirmation = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("date_confirmation")));
            String lives_in_Wuhan = Bytes.toString(result.getValue(FAMILY_NAME.getBytes(), Bytes.toBytes("lives_in_Wuhan")));


            if (age == null || sex == null || country == null) continue;


            lines.append(age);
            lines.append(";");
            lines.append(sex);
            lines.append(";");
            lines.append(city);
            lines.append(";");
            lines.append(province);
            lines.append(";");
            lines.append(country);
            lines.append(";");
            lines.append(latitude);
            lines.append(";");
            lines.append(longitude);
            lines.append(";");
            lines.append(date_admission_hospital);
            lines.append(";");
            lines.append(date_confirmation);
            lines.append(";");
            lines.append(lives_in_Wuhan);
            lines.append("\n");


        }



        System.out.println(lines);
        return lines;

    }


}