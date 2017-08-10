package com.meizu.algo.classify.article

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by linjiang on 2017/7/12.
  */
class ArticleClassifierOfflineModel(categoryMap: Map[Int, String], weight: Array[Double]) extends Serializable {
  require(weight.length / (categoryMap.size - 1) == 201, "weight or cat length error")

  def classify(title: String, content: String, wordVectors: Map[String, Array[Double]]): String = {
    val titleSeg = Segment.segment(title)
    val contentSeg = Segment.segment(content)
    return ArticleClassifier.classify(titleSeg, contentSeg, wordVectors, categoryMap, weight)
  }
}

object ArticleClassifierOfflineModel {
  /**
    * 查询词向量
    * @param hiveContext
    * @param partitionName
    * @return
    */
  def getWordVectors(hiveContext: HiveContext, partitionName: String): Map[String, Array[Double]] = {
    val sql = s"select word,vec from algo.lj_article_word_vec2 where stat_date='$partitionName'"
    hiveContext.sql(sql).map(r => (r.getString(0), r.getString(1).split(",").map(_.toDouble))).collect().toMap
  }
}
