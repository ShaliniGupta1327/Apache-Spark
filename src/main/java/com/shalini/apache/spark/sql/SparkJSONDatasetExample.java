package com.shalini.apache.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkJSONDatasetExample {
public static void main(String[] args) {
	// 1.Creating a spark session
	SparkSession sparksession=SparkSession.builder().appName("json file reader").master("local[2]").getOrCreate(); 
	// 2.Reading the JSON file  
	Dataset<Row> rawDataset=sparksession.read().json("D:\\project\\spark\\ws\\spark\\src\\main\\resource\\input\\MOCK_DATA .json");
	// 3.Applying filter on "id" and "salary" column and converting $ currency to €
	Dataset<Row> filtered_ds = rawDataset.filter(rawDataset.col("id").isNotNull()).filter(rawDataset.col("id").gt(0))
			.filter(rawDataset.col("salary").isNotNull()).filter(rawDataset.col("salary").notEqual(""))
			.withColumn("salary", functions.regexp_replace(rawDataset.col("salary"), "[$]", "").cast("Double").multiply(0.905254).cast("String"));
	// 4. Adding current_timestap and adding euro symbol to salary column
	Dataset<Row> cleansed_ds = filtered_ds.withColumn("Status", functions.lit("Cleansed"))
			.withColumn("current_timestamp", functions.current_timestamp().cast("string"))
			.withColumn("salary", functions.concat(functions.lit("€"), filtered_ds.col("salary"))).filter(filtered_ds.col("salary").isNotNull())
			.drop("_corrupt_record");
	// 5.Writing filtered Dataset to specified location
	cleansed_ds.write().mode("overwrite").format("json").save("D:\\project\\spark\\ws\\spark\\src\\main\\resource\\output");
	cleansed_ds.show();
	
}
}
