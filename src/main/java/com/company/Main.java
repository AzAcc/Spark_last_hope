package com.company;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;

public class Main {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Spark_hive"), conf);
        Path path = new Path("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Spark_hive");
        if (hdfs.exists(path)){
            hdfs.delete(path,true);
        }
        long startTime = System.nanoTime();
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Spark SQL App")
                .config("spark.sql.warehouse.dir", "hdfs://cdh631.itfbgroup.local:8020/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        spark.catalog().listDatabases();
        spark.sql("create table az.all_steps from(select distinct step_id from az.event_data_train)");
        spark.sql("select * from (SELECT user_id,count(*) as passed_steps from az.event_data_train where action =\"passed\" group by user_id) where passed_steps = (select count(*) from all_steps)")
                .repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Spark_hive");
        spark.sql("drop table az.all_steps");
        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        double seconds = (double)totalTime / 1_000_000_000.0;
        System.out.println(seconds);
    }
}
