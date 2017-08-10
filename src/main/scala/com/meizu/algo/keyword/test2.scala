package com.meizu.algo.keyword

import java.io.{BufferedReader, InputStreamReader}
import java.text.SimpleDateFormat
import java.util.Date

import com.meizu.algo.classify.article.{ArticleClassifier, ArticleClassifierOfflineModel, Segment}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import utils.{SparkApp, StrUtil}
import org.apache.hadoop.fs.Path

import scala.collection.mutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/7/4. 产生关键词和分类存入数据库中
  */
object test2 {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  var sc: SparkContext = null
  val weitht_path = "/user/mzsip/recommend_nearline/article_tag/20170718/classify_weight/classify_weight"
  val category_path = "/user/mzsip/recommend_nearline/article_tag/20170718/category/category"

  def main(args: Array[String]): Unit = {
    val userName = "mzsip"
    System.setProperty("user.name", userName)
    System.setProperty("HADOOP_USER_NAME", userName)
    println("***********************start*****************************")
    val sparkConf: SparkConf = new SparkConf().setAppName("xueyuan_clustertext")
    sc = new SparkContext(sparkConf)
    println("***********************sc*****************************")
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    val count = args(0)
    println("********************" + count + "***********************")
    val sqlContext = SparkApp.hiveContext
    val selectSrc = "select a.fid,a.ftitle,cast(unbase64(a.fcontent) as string) from mzreader.ods_t_article_c a where  a.fid is not null and a.ftitle is not null and a.fcontent is not null and  a.fkeywords is not null limit " + count


    val srcData = sqlContext.sql(selectSrc)
    val id_title_content = srcData.map(r => {
      (r.getLong(0), r.getString(1), r.getString(2))
    })
    id_title_content.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************id_title_content_count= " + id_title_content.count() + "*****************************")


    val category = getCategoryMap()
    val weight = getWeight()
    val model = new ArticleClassifierOfflineModel(category.toMap, weight)
    val word_vec = KeyWord.queryWordVec("20170804")
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "********************word_vec=" + word_vec.size + "***********************")
    val word_idf = KeyWord.queryIdf()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "********************word_idf=" + word_idf.size + "***********************")
    val word_vec_br = sc.broadcast(word_vec)
    val word_idf_br = sc.broadcast(word_idf)

    val result2 = id_title_content.repartition(400).mapPartitions(iter => for (r <- iter) yield {
      val id = r._1
      val title = r._2
      val content = r._3
      val time0 = System.currentTimeMillis()
      val kw = KeyWord.get_kw(title, content, word_vec_br.value, word_idf_br.value, Array(5, 2, 3))
      val time1 = System.currentTimeMillis()
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "********************kw time=" + (time1 - time0) + "***********************")
      val cate = model.classify(title, content, word_vec_br.value)
      val time2 = System.currentTimeMillis()
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "********************cate time=" + (time2 - time1) + "***********************")
      val tit_se = KeyWord.Segment.segment(title)
      val cont_se = KeyWord.Segment.segment(content)


      //      val (tit_se,cont_se)=KeyWord.seg(title,content)
      val i = content.indexOf("<p>")
      val j = content.indexOf("</p>")
      var p1 = ""
      if (i >= 0 && j >= 0 && i < j) {
        p1 = content.substring(i + 1, j)
      }
      val p1_se = KeyWord.Segment.segment(p1)
      (id, tit_se.mkString(","), p1_se.mkString(","), cont_se.mkString(","), kw.mkString(","), cate)
      //      (id, kw.mkString(","), cate)
    })
    result2.cache()
    val size = result2.count()
    id_title_content.unpersist()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "********************size=" + size + "***********************")

    val result = result2.map(r => Row(r._1, r._2, r._3, r._4, r._5, r._6))

    val schema = StructType(List(
      StructField("id", LongType),
      StructField("title", StringType),
      StructField("p1", StringType),
      StructField("content", StringType),
      StructField("kw", StringType),
      StructField("cate", StringType))
    )

    result.repartition(100).persist()
    SparkApp.saveToHive(result, "algo.xueyuan_tit_p1_con_key_cate", schema, "20170810")
    result2.unpersist()
    //
    //    //    Log.info("finish ******")

  }

  def getWeight() = {
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val path1 = new Path(weitht_path)
    val reader1 = new BufferedReader(new InputStreamReader(hdfs.open(path1), "utf-8"))
    var weight = ""
    var line1 = reader1.readLine()
    while (line1 != null) {
      if (!line1.equals("null")) {
        weight += line1.trim
      }
      line1 = reader1.readLine()
    }
    weight.split(",").map(r => r.toDouble)
  }

  def getCategoryMap() = {
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val path1 = new Path(category_path)
    val reader1 = new BufferedReader(new InputStreamReader(hdfs.open(path1), "utf-8"))
    var cetegory_map = new HashMap[Int, String]()
    var line1 = reader1.readLine()
    while (line1 != null) {
      if (!line1.equals("null")) {
        val index_cetegory = line1.trim.split(":")
        if (index_cetegory.length == 2) {
          cetegory_map.put(index_cetegory(0).toInt, index_cetegory(1))
        }
      }
      line1 = reader1.readLine()
    }
    cetegory_map
  }
}
