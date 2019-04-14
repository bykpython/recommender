package com.atguigu.bigdata.itemrecom

import com.atguigu.bigdata.common.DataModel
import com.atguigu.bigdata.common.DataModel.{MongoConfig, Rating, Recommendation}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ItemCFrecom {

  /**
    * 同现相似度计算公式
    *
    * @param numOfRatersForAAndB
    * @param numOfRatersForA
    * @param numOfRatersForB
    * @return
    */
  def coocurence(numOfRatersForAAndB: Long, numOfRatersForA: Long, numOfRatersForB: Long): Double = {
    numOfRatersForAAndB / math.sqrt(numOfRatersForA * numOfRatersForB)
  }

  def main(args: Array[String]): Unit = {

    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个spark配置
    val conf: SparkConf = new SparkConf().setMaster(config.get("spark.cores").get).setAppName(this.getClass.getName.stripSuffix("$"))

    // 创建一个sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 调高日志等级
    spark.sparkContext.setLogLevel("ERROR")

    // 创建一个mongodb配置
    val mgConf = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

    // 从mongodb的rate(userId, productId, score, timestamp)获取数据
    val ratingDF: DataFrame = spark
      .read
      .option("uri", mgConf.uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .rdd
      .map {
        rating => {
          (rating.userId, rating.productId, rating.score)
        }
      }.cache()
      .toDF("userId", "productId", "rating")

/*
    val rateDF: DataFrame = spark
      .read
      .option("uri", mgConf.uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()

    // Rating(userId, productId, score, timestamp)
    val rateRDD: RDD[Rating] = rateDF.as[Rating].rdd

    val frame: DataFrame = rateRDD.map(rate=>(rate.userId, rate.productId, rate.score)).cache().toDF("userId", "productId", "rating")
*/

    // alias==as (productId, count|nor)
    val numRatersPerProduct: Dataset[Row] = ratingDF.groupBy("productId").count().alias("nor")

    println("numRatersPerProduct:")
    numRatersPerProduct.show(5)
    /**
      * 商品被评分的次数
      * numRatersPerProduct:
      * +---------+-----+
      * |productId|count|
      * +---------+-----+
      * |   505556|  172|
      * |   275707|  364|
      * |   469516|  312|
      * |   379243|  423|
      * |    66003|  264|
      * +---------+-----+
      */

    // 在原记录基础上加上product的打分者的数量
    //rate(userId, productId, score) join (productId, count),条件时productId相等
    // (userId, productId, score, count|nor) 此操作类似于开窗函数中的聚合
    val ratingsWithSize: DataFrame = ratingDF.join(numRatersPerProduct, "productId")

    println("ratingsWithSize:")
    ratingsWithSize.show(5)
    /**可以用sql的开窗函数来实现此效果
      * ratingsWithSize:
      * +---------+------+------+-----+
      * |productId|userId|rating|count|
      * +---------+------+------+-----+
      * |   505556| 13784|   3.0|  172|
      * |   505556| 23120|   5.0|  172|
      * |   505556|  3105|   5.0|  172|
      * |   505556|280572|   5.0|  172|
      * |   505556|   265|   5.0|  172|
      * +---------+------+------+-----+
      */

    // 执行内联操作
    val joinedDF: DataFrame = ratingsWithSize.join(ratingsWithSize, "userId")
      .toDF("userId", "product1", "rating1", "nor1", "product2", "rating2", "nor2")

    println("joinedDF:")
    joinedDF.show(5)

    /**
      * joinedDF:
      * +------+--------+-------+----+--------+-------+----+
      * |userId|product1|rating1|nor1|product2|rating2|nor2|
      * +------+--------+-------+----+--------+-------+----+
      * |   496|  379243|    4.0| 423|  379243|    4.0| 423|
      * |   496|  379243|    4.0| 423|   48907|    5.0| 325|
      * |   496|  379243|    4.0| 423|  286997|    5.0|3355|
      * |   496|  379243|    4.0| 423|   13316|    5.0|1136|
      * |   496|  379243|    4.0| 423|  184282|    5.0|1079|
      * +------+--------+-------+----+--------+-------+----+
      */

    joinedDF.selectExpr("userId", "product1", "nor1", "product2", "nor2").createOrReplaceTempView("joined")

    // 计算必要的中间数据，注意此处有where限定，只计算了一半的数据量
    // 既给product1评分过，又给product2评分过的用户个数
    val sql = "select product1, product2, count(userId) as size, first(nor1) as nor1, first(nor2) as nor2 from joined group by product1, product2"
    val sparkMartrix: DataFrame = spark.sql(
      """
      |select product1
      |, product2
      |, count(userId) as size
      |, first(nor1) as nor1
      |, first(nor2) as nor2
      |from joined
      |group by product1, product2
      """.stripMargin).cache()

    println("sparkMartrix:")
    sparkMartrix.show(5)
    /**
      * sparkMartrix:
      * +--------+--------+----+----+----+
      * |product1|product2|size|nor1|nor2|
      * +--------+--------+----+----+----+
      * |  352021|  438242|  19| 258| 321|
      * |  460360|    8195|  77| 343|1398|
      * |  253869|  109236|  52| 338| 738|
      * |   33864|  260348|  36| 389| 372|
      * |  206404|  126767|  39| 617| 263|
      * +--------+--------+----+----+----+
      */

    // 计算物品的相似度
    val sim: DataFrame = sparkMartrix.map(row => {
      val size: Long = row.getAs[Long](2)
      val numRaters1: Long = row.getAs[Long](3)
      val numRaters2: Long = row.getAs[Long](4)
      val cooc: Double = coocurence(size, numRaters1, numRaters2)
      (row.getInt(0), row.getInt(1), cooc)
    }).toDF("productId_01", "productId_02", "cooc")

    val simDF: DataFrame = sim.map(row => (row.getAs[Int]("productId_01")
      , row.getAs[Int]("productId_02")
      , row.getAs[Double]("cooc"))).rdd
      .map(x => (x._1, (x._2, x._3)))
      .groupByKey()
      .map {
        case (productId, items) => {
          val tuples: List[(Int, Double)] = items.toList.filter(x => {
            x._1 != productId
          }).sortWith {
            case (left, right) => {
              left._2 > right._2
            }
          }.take(5)

          val recommendations: List[Recommendation] = tuples.map(x => Recommendation(x._1, x._2))
          (productId, recommendations)
        }
      }.toDF()

    simDF
      .write
      .option("uri", mgConf.uri)
      .option("collection", DataModel.PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 关闭spark
    spark.close()

  }

}





















