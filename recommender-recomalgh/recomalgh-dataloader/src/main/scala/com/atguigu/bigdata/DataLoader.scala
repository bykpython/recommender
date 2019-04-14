package com.atguigu.bigdata

import com.atguigu.bigdata.common.DataModel
import com.atguigu.bigdata.common.DataModel.{MongoConfig, Product, Rating}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection, MongoDB}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {

  // 定义一个含有隐式参数的方法
  def storeDataInMongoDB(produceDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig) = {

    // 新建一个到mongodb的连接
    // 派生对象MongoClient.apply(MongoClientURI.apply(mongoConfig.uri))
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 如果mongodb中有对应的数据集（类似数据库的表），则删除
    //mongoClient.apply(mongoConfig.db).apply(MONGODB_PRODUCT_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(DataModel.MONGODB_PRODUCT_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(DataModel.MONGODB_RATING_COLLECTION).dropCollection()

    // 将当前数据写入到mongodb
    produceDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", DataModel.MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对数据集Products中的productId建立索引
    mongoClient(mongoConfig.db)(DataModel.MONGODB_PRODUCT_COLLECTION).createIndex(MongoDBObject("productId"->1))

    // 对数据集Rating中的userId、productId建立索引
    mongoClient(mongoConfig.db)(DataModel.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("userId"->1))
    mongoClient(mongoConfig.db)(DataModel.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("productId"->1))

    // 关闭mongodb的连接
    mongoClient.close()
  }

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建spark配置对象
    val conf: SparkConf = new SparkConf().setMaster(config.get("spark.cores").get).setAppName("DataLoader")

    // 创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 从指定文件读取商品信息数据
    val productRDD: RDD[String] = spark.sparkContext.textFile(DataModel.PRODUCTS_DATA_PATH)

    // 将行数据进行切分，获取所需字段，映射成Product类型
    val produceDF: DataFrame = productRDD.map(item => {
      val attr: Array[String] = item.split("\\^")

      Product(attr(0).toInt, attr(1).trim, attr(5).trim, attr(4).trim, attr(6).trim)
    }).toDF()

    // 从指定文件读取评分记录信息数据
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(DataModel.RATING_DATA_PATH)

    // 将行数据进行切分，获取所需字段，映射成Rateing类型
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr: Array[String] = item.split(",")

      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    // 设置隐式变量
    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    // 将商品信息Product和评分记录信息Rating存入到mongodb中，隐式变量mongoConfig也会传入进去
    storeDataInMongoDB(produceDF, ratingDF)

    // 关闭spark
    spark.stop()
  }
}
