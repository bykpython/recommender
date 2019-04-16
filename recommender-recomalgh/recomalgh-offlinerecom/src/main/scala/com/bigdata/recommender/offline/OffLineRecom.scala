package com.bigdata.recommender.offline

import com.atguigu.bigdata.common.DataModel
import com.atguigu.bigdata.common.DataModel.MongoConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.jblas.DoubleMatrix

object OffLineRecom {

  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix) : Double ={
    product1.dot(product2) / ( product1.norm2()  * product2.norm2() )
  }

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "reommender"
    )

    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores")).set("spark.executor.memory","6G").set("spark.driver.memory","2G")

    //基于SparkConf创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //创建一个MongoDBConfig
    val mongoconfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 导入隐式转换
    import spark.implicits._

    // 读取数据(userId, productId, score, timestamp)====>(userId, productId, score)
    val ratingRDD: RDD[(Int, Int, Double)] = spark
      .read
      .option("uri", mongoconfig.uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Rating]
      .rdd
      .map(rate => (rate.userId, rate.productId, rate.score)).cache()

    // 用户的数据集(userId)
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()

    // (productId, name, categories, imageUrl, tags)====>(productId)
    val productRDD: RDD[Int] = spark
      .read
      .option("uri", mongoconfig.uri)
      .option("collection", DataModel.MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Product]
      .rdd
      .map(_.productId).distinct().cache()

    // 创建训练数据集自定义的(userId, productId, score)===>类库的sparkRating(userId, productId, score)
    val trainData: RDD[Rating] = ratingRDD.map(x=>Rating(x._1, x._2, x._3))

    // r: M x N
    // u: M x K
    // i: K x N

    //rank参数是K参数，iterations是迭代次数，lambda是正则化系数
    val (rank ,iterations, lambda) = (50, 5, 0.01)

    // 训练ALS模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)


    // 计算用户推荐矩阵
    // 需要构造一个usersProducts RDD[(Int, Int)]
    // (userId).cartesian(productId)===>(userId, productId)
    val userProducts: RDD[(Int, Int)] = userRDD.cartesian(productRDD)


    val preRatings: RDD[Rating] = model.predict(userProducts)

    println("preRatings:")
    preRatings.toDF().show(10)


    val userRecs: DataFrame = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (userId, recs) => {
          val recommendations: List[DataModel.Recommendation] = recs.toList.sortWith(_._2 > _._2).take(DataModel.USER_MAX_RECOMMENDATION).map(x => DataModel.Recommendation(x._1, x._2))
          DataModel.UserRecs(userId, recommendations)
        }
      }.toDF()

    userRecs
      .write
      .option("uri", mongoconfig.uri)
      .option("collection", DataModel.USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (productId, features) => {
        (productId, new DoubleMatrix(features))
      }
    }

    val productRecs: DataFrame = productFeatures.cartesian(productFeatures)
      .filter {
        case (a, b) => {
          a._1 != b._1
        }
      }.map {
      case (a, b) => {
        val simScore: Double = this.consinSim(a._2, b._2)
        (a._1, (b._1, simScore))
      }
    }.filter(_._2._2 >= 0.6)
      .groupByKey()
      .map {
        case (productId, items) => {
          DataModel.ProductRecs(productId, items.toList.map(x => DataModel.Recommendation(x._1, x._2)))
        }
      }.toDF()


    productRecs
      .write
      .option("uri", mongoconfig.uri)
      .option("collection", DataModel.OFFLINE_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }
}

























