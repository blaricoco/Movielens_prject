package com.blarico.newday

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.log4j._
import org.apache.spark.sql.functions.{col, from_unixtime, round}
import org.apache.spark.sql.types.DateType

class MovielensParseData(val path: String) {

  //Initialize
  Logger.getLogger("movielens.logger").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Movielens")
    .getOrCreate()

  // Schemas
  val schemas: MovielensSchemas = new MovielensSchemas()

  //Movies Dataframe
  val movies: DataFrame = spark.read.format("csv")
    .option("delimiter", "::")
    .schema(schemas.moviesSchema)
    .load(path + "movies.dat")

  //Ratings Dataframe
  val ratings: DataFrame = spark.read.format("csv")
    .option("delimiter", "::")
    .schema(schemas.ratingsSchema)
    .load(path + "ratings.dat")
    .withColumn("Timestamp", from_unixtime(col("Timestamp")).cast(DateType))

  //Users Dataframe
  val users: DataFrame = spark.read.format("csv")
    .option("delimiter", "::")
    .schema(schemas.usersSchema)
    .load(path + "users.dat")
  

  //Drop unnecessary information
  val rated: DataFrame = ratings.drop("UserID", "Timestamp")

  //Find Max
  //val max: DataFrame = rated
  //  .groupBy("MovieID")
  //  .max("Rating")

  //Find Min
  //val min: DataFrame = rated
  //  .groupBy("MovieID")
  //  .min("Rating")

  //Find Average
  //val average: DataFrame = rated
  //  .groupBy("MovieID")
  //  .avg("Rating")
  //  .withColumn("avg(Rating)", round(col("avg(Rating)"), 2))

  //MovieRatings Dataframe
  //val movieRatings: DataFrame = movies.join(max, "MovieID")
  //  .join(min, "MovieID")
  //  .join(average, "MovieID")
  //  .withColumnRenamed("max(Rating)", "Max")
  //  .withColumnRenamed("min(Rating)", "Min")
  // .withColumnRenamed("avg(Rating)", "Average")
  //  .sort("MovieID")

  import org.apache.spark.sql.functions._

  val movieRatings: DataFrame = rated.groupBy("MovieID").agg(
    min("Rating"),
    max("Rating"),
    avg("Rating"))
    .withColumn("avg(Rating)", round(col("avg(Rating)"), 2))
    .withColumnRenamed("max(Rating)", "Max")
    .withColumnRenamed("min(Rating)", "Min")
    .withColumnRenamed("avg(Rating)", "Average")
    .join(movies, "MovieID")
    .sort("MovieID")



  //Top 3 Movies
  val topRated: Dataset[Row] = ratings.filter(ratings("Rating") === 5)
    .select("MovieID", "UserID")
    .groupBy("UserID")
    .agg(collect_set("MovieID"))
    .withColumnRenamed("collect_set(MovieID)", "TopMovies")
    .sort("UserID")



  def run(): Unit = {

  //Write dataframes to parquet format
  movies.write.mode("overwrite").parquet("movies.parquet")
  ratings.write.mode("overwrite").parquet("ratings.parquet")
  users.write.mode("overwrite").parquet("users.parquet")
  topRated.write.mode("overwrite").parquet("topRated.parquet")
  movieRatings.write.mode("overwrite").parquet("movieRatings.parquet")
    
  }
}
