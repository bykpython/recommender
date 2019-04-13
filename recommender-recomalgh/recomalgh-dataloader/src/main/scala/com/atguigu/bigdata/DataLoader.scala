package com.atguigu.bigdata

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection, MongoDB}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Product(productId: Int, name: String, categories: String, imageUrl: String, tags: String)

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

object DataLoader {

  val PRODUCTS_DATA_PATH = "D:\\tmpfile\\recommender\\recommender-recomalgh\\recomalgh-dataloader\\src\\main\\resources\\100products.csv"
  val RATING_DATA_PATH = "D:\\tmpfile\\recommender\\recommender-recomalgh\\recomalgh-dataloader\\src\\main\\resources\\9000_users_100_products_ratings.csv"

  val  MONGODB_PRODUCT_COLLECTION = "Products"
  val mONGODB_RATING_COLLECTION = "Rating"

  def storeDataInMongoDB(produceDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig) = {

    // 新建一个到mongodb的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //MongoClient.apply(MongoClientURI.apply(mongoConfig.uri))

    // 如果mongodb中有对应的数据库，则删除
    //mongoClient.apply(mongoConfig.db).apply(MONGODB_PRODUCT_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(mONGODB_RATING_COLLECTION).dropCollection()

    // 将当前数据写入到mongodb
    produceDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", mONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对数据表建立索引
    mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION).createIndex(MongoDBObject("productId"->1))
    mongoClient(mongoConfig.db)(mONGODB_RATING_COLLECTION).createIndex(MongoDBObject("userId"->1))
    mongoClient(mongoConfig.db)(mONGODB_RATING_COLLECTION).createIndex(MongoDBObject("productId"->1))

    // 关闭mongodb的连接
    mongoClient.close()
  }

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val conf: SparkConf = new SparkConf().setMaster(config.get("spark.cores").get).setAppName("DataLoader")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCTS_DATA_PATH)

    val produceDF: DataFrame = productRDD.map(item => {
      val attr: Array[String] = item.split("\\^")

      Product(attr(0).toInt, attr(1).trim, attr(5).trim, attr(4).trim, attr(6).trim)
    }).toDF()

    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr: Array[String] = item.split(",")

      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    storeDataInMongoDB(produceDF, ratingDF)

    spark.stop()
  }
}
