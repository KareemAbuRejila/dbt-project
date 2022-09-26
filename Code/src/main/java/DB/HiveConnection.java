package DB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class HiveConnection {

    public void InsertDataCOVID19(String filePath, String fileServerPath,String table) {
        //TODO:  Spark insert Data in Hive
        String warehouseLocation = new File(fileServerPath).getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .master("local")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .getOrCreate();
        spark.sql("LOAD DATA LOCAL INPATH '" + filePath + "' INTO TABLE default."+table);


        System.out.println("Data has been saved successfully");


    }


}