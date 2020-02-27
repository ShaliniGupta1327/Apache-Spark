package com.shalini.apache.spark.sql;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSparkSQLConnectoionClass {
public static void main(String[] args) throws ClassNotFoundException {
	// 1.Creating a spark session
	SparkSession spark=SparkSession.builder().appName("JavaSpark SQLConnection Program").master("local[2]").getOrCreate();
	// 2.Set the Properties
	Properties properties=new Properties();
	properties.put("user", "<username>");
	properties.put("password", "<password>");
	// 3.Give the url dabasetable name
	String url="jdbc:mysql://localhost:3306"; //change the url according to your JDBC connection
	String dbtable="schema.tablename";
	// 4.Driver Class
	Class.forName("com.mysql.jdbc.Driver");
	//5. Read the table
	Dataset<Row> datasetsql=spark.read().jdbc(url, dbtable, properties);
	// 6. show table
	datasetsql.show();
	
}
}
