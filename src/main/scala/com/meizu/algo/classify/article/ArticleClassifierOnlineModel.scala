package com.meizu.algo.classify.article

/**
  * 文章分类模型
  *
  * @param categoryMap 分类标签参数
  * @param weight 模型参数
  *
  * Created by linjiang on 2017/7/12.
  */
class ArticleClassifierOnlineModel(categoryMap: Map[Int, String], weight: Array[Double]) {
  def segment(text: String): Array[String] = {
    Segment.segment(text)
  }

  def classify(title: Array[String], content: Array[String], wordVectors: Map[String, Array[Double]]): String = {
    ArticleClassifier.classify(title, content, wordVectors, categoryMap, weight)
  }
}
