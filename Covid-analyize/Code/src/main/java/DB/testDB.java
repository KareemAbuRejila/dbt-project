package DB;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import org.apache.spark.sql.Row;

public class testDB {

	public static void main(String[] args) throws IOException {
		//ID,age,sex,city,province,country, latitude,longitude,date_admission_hospital,date_confirmation,lives_in_Wuhan

		new HiveConnection().InsertDataCOVID19("/home/cloudera/Desktop/Project/BigDataProject/src/tmp2/output.csv",
				"/user/hive/COVID19LINES","covid19LINES");

    }

}
