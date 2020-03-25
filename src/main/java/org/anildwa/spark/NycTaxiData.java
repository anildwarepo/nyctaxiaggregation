package org.anildwa.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import java.io.BufferedOutputStream;

/**
 * NycRideCount!
 *
 */
public class NycTaxiData {

  private static Logger log;
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: NycWordCount file");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("NycRideCount").getOrCreate();
    
    
    log = org.apache.log4j.LogManager.getLogger("myLogger");

    StructType schema = new StructType().add("VendorID", "integer").add("lpep_pickup_datetime", "string")
        .add("lpep_dropoff_datetime", "string").add("store_and_fwd_flag", "string").add("ratecodeID", "string")
        .add("Pickup_longitude", "string").add("Pickup_latitude", "string").add("Dropoff_longitude", "string").add("Dropoff_latitude", "string").add("passenger_count", "string")
        .add("trip_distance", "string").add("fare_amount", "string").add("extra", "string").add("mta_tax", "string")
        .add("tip_amount", "string").add("tolls_amount", "string").add("ehail_fee", "string")
        .add("improvement_surcharge", "string").add("total_amount", "double").add("payment_type", "string")
        .add("trip_type", "string").add("congestion_charge", "string");

    double startTime = System.currentTimeMillis();
    Dataset<Row> df = spark.read().option("mode", "DROPMALFORMED").schema(schema).csv(args[0]);


    df.cache();
    df.count();
    // group by VendorID and count totalamount and VendorID
    Dataset<Row> dfResult = df.groupBy("VendorID").agg(sum("total_amount"), count("VendorID"));

    dfResult.coalesce(1).write().mode(SaveMode.Overwrite).csv(args[1]);
    

    double elapsedTime = System.currentTimeMillis() - startTime;

    log.info("anildwalogging time:" + Double.toString(elapsedTime));
    Log(Double.toString(elapsedTime));

    spark.stop();

  }

  private static void Log(String str)
  {
    try {

      log.info("anildwalogging message");
      Configuration conf = new Configuration();
      String hdfsUri = "abfss://nytaxidata@anildwaadlsv2.dfs.core.windows.net/";
      conf.set("fs.azure.account.key.anildwaadlsv2.dfs.core.windows.net", hdfsUri);
      FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
      FSDataOutputStream output = fs.create(new Path("metrics.txt"));
      output.writeUTF("elapsed time in milliseconds: " + str);
      //output.hsync();
      output.flush();
      output.close();

    } catch (IOException e) {

      log.error("anildwalogging exception:" + e.toString());
    }
  }

  
}
