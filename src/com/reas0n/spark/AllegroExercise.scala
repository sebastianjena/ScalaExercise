package com.reas0n.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object AllegroExercise {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "AllegroEx")
  
    //
  }
}