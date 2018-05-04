package com.reas0n.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._



object AllegroExercise {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "AllegroEx")
  
    // import data with contract award notices using spark-csv library 
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("TED_CAN_2017.csv")

        
    /* clean up data & delete canceled/not awarded orders. 
     * also awards for multiple countries are not measurable, so we filter them out
     */
    val cleanDs1 = df.dropDuplicates("ID_AWARD")
        .select("CANCELLED", "INFO_ON_NON_AWARD", "WIN_COUNTRY_CODE", "AWARD_VALUE_EURO_FIN_1", "B_MULTIPLE_COUNTRY")
        .filter(x => {x(0) == 0 && x(1) == null && x(2) != null && x(3) != null && x(4) == "N"})

    
    // data was still dirty, WIN_COUNTRY_CODE had values like PL---PL---PL,
    // so we need to clean it up more
    val tidyUpCountryCode = udf((value: String) => {
        val x = value.split("---").distinct
        x(0)  
    })
    val cleanDf1 = cleanDs1.withColumn("WIN_COUNTRY_CODE", tidyUpCountryCode(col("WIN_COUNTRY_CODE")))
    
    /* get sure we have proper country codes (actually the .csv is really 
     * bad and has errors like country named "1A". some awards are also 0EUR so we take them out
     */
    val isOrdinary = udf((value: String) => {
        val x = value.forall(_.isLetter)
        x
    })
    val cleanDs2 = cleanDf1.where(isOrdinary(col("WIN_COUNTRY_CODE")) === true && length(col("WIN_COUNTRY_CODE")) === 2 && col("AWARD_VALUE_EURO_FIN_1") > 0)
    
    
    // add order id, it will be helpful to count average amount of orders
    val idDf = cleanDs2.withColumn("ORDER", monotonically_increasing_id)

    // save to file the final dataframe with sum/avg of orders amounts and amount of orders in each country
    val sortedDf = idDf.groupBy("WIN_COUNTRY_CODE").agg(sum("AWARD_VALUE_EURO_FIN_1"), avg("AWARD_VALUE_EURO_FIN_1"),count("ORDER"))
    sortedDf.sort("WIN_COUNTRY_CODE").coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .mode("overwrite")
        .save("result.csv")   
    
        
    // count average amount of orders and print it    
    val sumOrdersAmount : Long = sortedDf.agg(sum("count(ORDER)").cast("long")).first.getLong(0)
    val countriesAmount : Long = sortedDf.agg(count("count(ORDER)").cast("long")).first.getLong(0)
    
    println("sum amount of orders - " + sumOrdersAmount)
    println("amount of countries - " + countriesAmount)
    println("average amount of orders - " + (sumOrdersAmount/countriesAmount))
    
  }
}