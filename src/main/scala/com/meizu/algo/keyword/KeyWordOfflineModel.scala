package com.meizu.algo.keyword

/**
  * Created by xueyuan on 2017/7/17.
  */
class KeyWordOfflineModel() {

  def getWordIdf() = {
    KeyWord.queryIdf()
  }

  def getKeyWord(title: String, content: String, wordVec: Map[String, Array[Double]], wordIdf: Map[String, Double], weight: Array[Double]) = {
    KeyWord.get_kw(title, content, wordVec, wordIdf, weight)
  }
}
