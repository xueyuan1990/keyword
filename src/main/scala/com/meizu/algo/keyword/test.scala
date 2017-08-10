package com.meizu.algo.keyword

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import utils.{SparkApp, StrUtil}
import org.apache.hadoop.fs.Path

import scala.collection.mutable

/**
  * Created by xueyuan on 2017/7/4.
  */
object test {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  var sc: SparkContext = null

  def main(args: Array[String]): Unit = {
    //    val cont_se = KeyWord.Segment.segment("我今年2岁,我今年3岁,我是一名学生")
    //    println(cont_se.mkString(","))
    val title = "李时珍为何弃文从医? 乡试多次均失败 体弱多病"
    val content = "稍微读了点书的人，大多知道大作家鲁迅弃医从文的故事，然而大家却未必知道李时珍在从医之前也经过了一番艰苦的思想斗争。只不过他不是弃医从文，恰恰相反，他是弃文从医。</p><p>李时珍之所以在中国的历史上，尤其是医学史上具有极其崇高的地位，开始并没把从医当成自己的理想。李时珍的父亲与爷爷都是相当不错的医生，他们都希望自己家里出一个读书人，希望有一个人能够通过科举考试考取功名，走上仕途，不再行医。可是这个希望一再地落空，最后他们都把希望寄托在李时珍身上。然而令人遗憾的是，李时珍只考络配图</p><p>落榜后的李时珍读书更加刻苦，不久他娶了一个大户人家的小姐吴氏为妻，妻子并未反对他考取功名，还经常鼓励他。公元1537年李时珍第二次参加了在武昌举行的乡试，这一次，他再次榜上无名。</p><p>要不要再考呢?这时的李时珍有些犹豫，开始思考这个问题网络配图</p><p>3年后的秋天，他第三次来到武昌参加乡试，再次失败。从此，李时珍对科举考试失去了兴趣，甚至对此产生了一种恐惧心理。也是从这时候起，他暗下决心，不再参加乡试，跟随父亲学医。</p><p>其实每个人的人生都不可能一帆风顺，即便是成功人士也不例外。当自己在人生遭遇了重大创伤的时候，需要的是勇敢地去面对，并及时调整好自己的心态，积极地投入到新的生活当中去。"
    val wordVec = new mutable.HashMap[String, Array[Double]]()
    wordVec.put("李时珍", Array[Double](1, 2))
    wordVec.put("乡试", Array[Double](2, 2))
    wordVec.put("作家", Array[Double](1, 2))
    wordVec.put("鲁迅", Array[Double](2, 2))
    val wordIdf = new mutable.HashMap[String, Double]()
    wordIdf.put("李时珍", 1)
    wordIdf.put("乡试", 1)
    wordIdf.put("作家", 1)
    wordIdf.put("鲁迅", 1)
    //    //online
    val model_online = new KeyWordOnlineModel()
    //分词
    val time2 = System.currentTimeMillis()
    //    //计算关键词
    val kw_online = KeyWord.get_kw(title, content, wordVec.toMap, wordIdf.toMap, Array(5, 2, 3))
    val kw_online2 = KeyWordOld.get_kw(title, content, wordVec.toMap, wordIdf.toMap, Array(5, 2, 3))
    val time3 = System.currentTimeMillis()


    //    //offline
    //    val model_offline = new KeyWordOfflineModel(title, content)
    //    //wordIdf
    //    val wordIdf_offline = model_offline.getWordIdf()
    //    //计算关键词
    //    val kw_offline = model_offline.getKeyWord(wordVec, wordIdf)
    //    val kw_online = KeyWord.get_kw(title, content, wordVec.toMap, wordIdf)
    //    val seg_online = KeyWord.seg_dis(title, content)
    //    println("***********kw*************")
    //
        println(kw_online.mkString(","))
    //    println("***********kw2*************")
        println(kw_online2.mkString(","))
    //    println("***********seg*************")
    //    println(seg_online.mkString(","))
    println("finish")
  }

  def main1(args: Array[String]): Unit = {
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
    //    val selectSrc = "select a.fid,a.ftitle,cast(unbase64(a.fcontent) as string),b.tags from mzreader.ods_t_article_c a ,algo.news_recommend_item_tags_source b where a.fid=b.item and a.fid is not null and a.fkeywords is not null and a.fkeywords != ''  and b.stat_date=20170725  limit " + count
    val selectSrc = "select 1L,title,article,keyword from algo.xueyuan_article_key_tencent  where title is not null and  article is not  null  "
    //    val selectSrc = "select a.fid,a.ftitle,cast(unbase64(a.fcontent) as string),b.tags from mzreader.ods_t_article_c a where  a.fid is not null and a.fkeywords is not null and a.fkeywords != '' and a.stat_date=20170726 limit " + count
    val srcData = sqlContext.sql(selectSrc)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************srcData_count= " + srcData.count() + "*****************************")
    val id_title_content = srcData.map(r => {
      (r.getLong(0), r.getString(1), r.getString(2), r.getString(3))
    })
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************id_title_content_count= " + id_title_content.count() + "*****************************")

    val word_vec = KeyWord.queryWordVec("20170714")
    println("********************word_vec=" + word_vec.size + "***********************")
    val word_idf = KeyWord.queryIdf()
    println("********************word_idf=" + word_idf.size + "***********************")
    val word_vec_br = sc.broadcast(word_vec)
    val word_idf_br = sc.broadcast(word_idf)
    val result2 = id_title_content.repartition(10).mapPartitions(iter => for (r <- iter) yield {
      val id = r._1
      val title = r._2
      val content = r._3
      val tags = r._4
      val kw = KeyWord.get_kw(title, content, word_vec_br.value, word_idf_br.value, Array(5, 2, 3))
      (id, title, KeyWord.Segment.stripHtml(content), kw.mkString(","), tags)
    })
    //    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************result2_count= " + result2.length + "*****************************")
    //    val result2 = id_title_content.repartition(10).mapPartitions(iter => for (r <- iter) yield {
    //      val id = r._1
    //      val title = r._2
    //      val content = r._3
    //      val tags = r._4
    //      val kw = ""
    //      (id, title, KeyWord.stripHtml(content), kw.mkString(","), tags)
    //    }).collect()
    //    save(outputFile, result2)
    //    for ((id, tit, con, kw, tags) <- result2) {
    //      println("id:" + id + " tit:" + tit + " con:" + con + " kw" + kw + " tags:" + tags)
    //    }
    //    for ((id, tit, con, kw, tags) <- result2.take(10)) {
    //      println(id + " " + tit + " " + con + " " + kw + " " + tags)
    //    }
    val result = result2.map(r => Row(r._1, r._2, r._3, r._4, r._5))
    //    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************result_count= " + result.count() + "*****************************")


    //    println("********************result=" + result.count() + "***********************")
    //    val schema = StructType(List(
    //      StructField("id", LongType),
    //      StructField("title", StringType),
    //      StructField("content", StringType),
    //      StructField("kw", StringType)))
    //    SparkApp.saveToHive(result, "algo.xueyuan_article_key_online", schema, "20170718")
    //    val title = "李时珍为何弃文从医? 乡试多次均失败 体弱多病"
    //    val content = "稍微读了点书的人，大多知道大作家鲁迅弃医从文的故事，然而大家却未必知道李时珍在从医之前也经过了一番艰苦的思想斗争。只不过他不是弃医从文，恰恰相反，他是弃文从医。</p><p>李时珍之所以在中国的历史上，尤其是医学史上具有极其崇高的地位，开始并没把从医当成自己的理想。李时珍的父亲与爷爷都是相当不错的医生，他们都希望自己家里出一个读书人，希望有一个人能够通过科举考试考取功名，走上仕途，不再行医。可是这个希望一再地落空，最后他们都把希望寄托在李时珍身上。然而令人遗憾的是，李时珍只考络配图</p><p>落榜后的李时珍读书更加刻苦，不久他娶了一个大户人家的小姐吴氏为妻，妻子并未反对他考取功名，还经常鼓励他。公元1537年李时珍第二次参加了在武昌举行的乡试，这一次，他再次榜上无名。</p><p>要不要再考呢?这时的李时珍有些犹豫，开始思考这个问题网络配图</p><p>3年后的秋天，他第三次来到武昌参加乡试，再次失败。从此，李时珍对科举考试失去了兴趣，甚至对此产生了一种恐惧心理。也是从这时候起，他暗下决心，不再参加乡试，跟随父亲学医。</p><p>其实每个人的人生都不可能一帆风顺，即便是成功人士也不例外。当自己在人生遭遇了重大创伤的时候，需要的是勇敢地去面对，并及时调整好自己的心态，积极地投入到新的生活当中去。"
    //    val wordVec = new mutable.HashMap[String, Array[Double]]()
    //    wordVec.put("李时珍", Array[Double](1, 2))
    //    wordVec.put("乡试", Array[Double](2, 2))
    //    wordVec.put("作家", Array[Double](1, 2))
    //    wordVec.put("鲁迅", Array[Double](2, 2))
    //    val wordIdf = new mutable.HashMap[String, Double]()
    //    wordIdf.put("李时珍", 1)
    //    wordIdf.put("乡试", 1)
    //    wordIdf.put("作家", 1)
    //    wordIdf.put("鲁迅", 1)
    //    //    //online
    //    val model_online = new KeyWordOnlineModel()
    //    //分词
    //    val time2 = System.currentTimeMillis()
    //    //    //计算关键词
    //    val kw_online = model_online.getKeyWord(null, content, wordVec.toMap, wordIdf.toMap, Array(5, 2, 3))
    //    val kw_online2 = model_online.getKeyWord(title, content, wordVec.toMap, wordIdf.toMap, Array(5, 2, 3))
    //    val time3 = System.currentTimeMillis()


    //    //offline
    //    val model_offline = new KeyWordOfflineModel(title, content)
    //    //wordIdf
    //    val wordIdf_offline = model_offline.getWordIdf()
    //    //计算关键词
    //    val kw_offline = model_offline.getKeyWord(wordVec, wordIdf)
    //    val kw_online = KeyWord.get_kw(title, content, wordVec.toMap, wordIdf)
    //    val seg_online = KeyWord.seg_dis(title, content)
    //    println("***********kw*************")
    //
    //    println(kw_online.mkString(","))
    //    println("***********kw2*************")
    //    println(kw_online2.mkString(","))
    //    println("***********seg*************")
    //    println(seg_online.mkString(","))


    //    println((time2 - time1) + "," + (time3 - time2))
    val schema = StructType(List(
      StructField("id", LongType),
      StructField("title", StringType),
      StructField("content", StringType),
      StructField("kw", StringType),
      StructField("tags", StringType))
    )

    result.repartition(1).persist()
    SparkApp.saveToHive(result, "algo.xueyuan_article_key_compare_tencent", schema, "20170731")
    srcData.unpersist()
    //
    //    //    Log.info("finish ******")

  }

  def stripHtml(content: String): String = {
    if (StrUtil.isEmpty(content)) ""
    else {
      content.replaceAll("<p>", "")
        .replaceAll("</p>", "")
        .replaceAll("<img src[^>]/>", "")
        .replaceAll("<body>", "")
        .replaceAll("<html>", "")
        .replaceAll("</body>", "")
        .replaceAll("</html>", "")
    }
  }

}
