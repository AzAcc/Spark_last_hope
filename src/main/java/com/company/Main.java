package com.company;
import org.apache.spark.sql.SparkSession;
public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Simple Spark SQL App")
                .config("spark.sql.warehouse.dir", "hdfs://cdh631.itfbgroup.local:8020/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        spark.catalog().listDatabases();
    }
}
