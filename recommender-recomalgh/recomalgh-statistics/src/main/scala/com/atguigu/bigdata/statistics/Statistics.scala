package com.atguigu.bigdata.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @param productId  商品id
  * @param name       商品名称
  * @param categories 商品类别
  * @param imageUrl   商品图片url
  * @param tags       商品标签
  */
case class Product(productId:Int, name:String, categories:String, imageUrl:String, tags:String)

/**
  *
  * @param userId     用户id
  * @param productId  商品id
  * @param score      商品评分
  * @param timestamp  商品评分时间
  */
case class Rating(userId:Int, productId:Int, score:Double, timestamp:Int)

/**
  *
  * @param uri        数据库uri
  * @param db         数据库名
  */
case class MongoConfig(uri:String, db:String)

/**
  *
  * @param rid        推荐的商品id
  * @param r          推荐指数
  */
case class Recommendation(rid:Int, r:Double)

object Statistics {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_COLLECTION = "Products"

  // 统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"
  val TOP_PRODUCTS = "GenresTopProducts"

  // 入口方法
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" ->"local[*]",
      "mongo.uri" ->"mongodb://localhost:27017/recommender",
      "mongo.db"->"recommender"
    )

    // 创建spark配置对象
    val conf: SparkConf = new SparkConf().setMaster(config.get("spark.cores").get).setAppName("Statistics")

    // 创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 调高日志等级
    spark.sparkContext.setLogLevel("ERROR")

    // 先得到连接mongo的配置
    val mgConf = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    // 从mongodb中获取评分记录数据
    val ratingDF: DataFrame = spark
      .read
      .option("uri", mgConf.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    // 从mongodb中获取商品信息
    val productDF: DataFrame = spark
      .read
      .option("uri", mgConf.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .toDF()

    // 为评分记录数据创建一个临时视图--评分记录表ratings
    ratingDF.createOrReplaceTempView("ratings")

    // 从ratings查询数据形成结构--->(productId, count)：商品被评分的次数
    // 按照商品id进行分组
    val rateMoreProductsDF: DataFrame = spark.sql("select productId, count(productId) as count from ratings group by productId")

    // 将分组后的商品评分数据存入mongodb
    rateMoreProductsDF
      .write
      .option("uri", mgConf.uri)
      .option("collection", RATE_MORE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 自定义一个udf函数，用于处理时间
    val spFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x:Int)=>spFormat.format(new Date(x*1000L)).toInt)

    // 使用自定义udf函数，从评分记录表中查数据
    val ratingOfYearMonth: DataFrame = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")

    // 为查出来的数据创建临时视图ratingOfMonth
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // 将ratingOfMonth视图数据按照月份、商品id进行分组聚合
    val rateMoreRecentlyProducts: DataFrame = spark.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId")

    // 将聚合后的数据存入mongodb
    rateMoreRecentlyProducts
      .write
      .option("uri", mgConf.uri)
      .option("collection", RATE_MORE_RECENTLY_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 从评分记录表中，按照商品id分组，获取每一个商品的平均的评分
    val averageProductDF: DataFrame = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")

    // 将每一个商品的平均评分存入到mongodb中
    averageProductDF
      .write
      .option("uri", mgConf.uri)
      .option("collection", AVERAGE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }
}


