package xueyuan

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.github.fommil.netlib.F2jBLAS
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import lingc.ExpTable
import utils.{Blas, Log, SparkApp, StrUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/7/4.
  */
object test {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val htmlPatter = "<[^>]+?>"
  val WORD_SEP = ","
  val LINE_SEP = WORD_SEP + "###" + WORD_SEP
  var count = 100
  val sc = SparkApp.sc
  val sqlContext = SparkApp.hiveContext

  def main(args: Array[String]): Unit = {
    var fid = args(0)
    val srcTable = "mzreader.ods_t_article_c"

    val selectSrc = s"select a.fid,a.ftitle,cast(unbase64(a.fcontent) as string) from $srcTable a, algo.xueyuan_article_key b where b.stat_date='20170703' and a.fid=b.id and a.fresource_type=2  and a.fid is not null and a.fkeywords is not null and a.fkeywords != '' and fid=" + fid + " limit 1 "
    val srcData = sqlContext.sql(selectSrc)

    val data = srcData.map(r => {
      (r.getString(1), r.getString(2))
    }).map(r => (r._1, r._2)).take(1)
    val (title, content) = data(0)
    var w1 = 4
    var w2 = 2
    var w3 = 4

    val stopWord = kw_online.queryStopWord()

    val wordIdf = kw_online.queryIdf()
    val wordVec = kw_online.queryWordVec(stopWord)

    val start = System.currentTimeMillis()

    val kw = kw_online.process(title, content, wordVec, wordIdf, w1, w2, w3)
    println(kw.mkString(","))
    val end = System.currentTimeMillis()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************content= " + content + "*****************************")
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************title= " + title + "*****************************")
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************time= " + (end - start) + "*****************************")


  }


}
