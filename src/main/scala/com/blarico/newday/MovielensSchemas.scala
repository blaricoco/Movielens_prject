package com.blarico.newday

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class MovielensSchemas {

  //Movies Schema
  val moviesSchema = new StructType()
    .add("MovieID", IntegerType,true)
    .add("Title", StringType,true)
    .add("Genres", StringType,true)

  //Ratings Schema
  val ratingsSchema = new StructType()
    .add("UserID",IntegerType, true)
    .add("MovieID",IntegerType, true)
    .add("Rating",IntegerType, true)
    .add("Timestamp",IntegerType, true)

  //Users Schema
  val usersSchema = new StructType()
    .add("UserID", IntegerType, true)
    .add("Gender", StringType, true)
    .add("Age", IntegerType, true)
    .add("Occupation", IntegerType, true)
    .add("Zip-code", IntegerType, true)

}
