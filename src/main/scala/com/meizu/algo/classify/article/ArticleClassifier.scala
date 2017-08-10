package com.meizu.algo.classify.article

import com.hankcs.hanlp.HanLP

import scala.collection.mutable

/**
  * Created by linjiang on 2017/7/11.
  */
private object ArticleClassifier {

  private val UNKNOW = "unknow"

  def classify(title: Array[String], content: Array[String],
               wordVec: Map[String, Array[Double]], category: Map[Int, String], weight: Array[Double]): String = {
    val kw = getKw(title, content, wordVec)
    if (kw.isEmpty) return UNKNOW

    val vec = getVector(kw, wordVec)
    val (prob, result) = predict(vec, category, weight)
    if (prob < 0.5) return UNKNOW
    return result
  }

  private def getKw(title: Array[String], content: Array[String], wordVec: Map[String, Array[Double]]) = {
    val allWords = (title ++ content).toSet.toArray.filter(wordVec.contains(_))

    val score = new mutable.HashMap[String, Double]()
    for (i <- 0 until allWords.length) {
      val w = allWords(i)
      val wv1 = wordVec(w)
      val s =
        for (j <- 0 until allWords.length if i != j) yield {
          val wv2 = wordVec(allWords(j))
          cos(wv1, wv2)
        }
      score.put(w, s.sum)
    }
    val scoreRet = score.toArray.sortWith(_._2 > _._2).take(3).map(_._1)
    scoreRet
  }

  private def getVector(kw: Array[String], wordVec: Map[String, Array[Double]]): Array[Double] = {

    val vec = Array.fill(201)(0.0)
    vec(200) = 1
    kw.foreach(w => {
      val v = wordVec(w)
      for (i <- 0 until v.length) {
        vec(i) += v(i) / kw.length
      }
    })
    vec
  }


  private def predict(data: Array[Double], cat: Map[Int, String], weight: Array[Double]) = {

    val numClasses = (weight.length / data.length)
    val margins = (0 until numClasses).map { i =>
      val margin = dot(data, weight.slice(i * data.length, (i + 1) * data.length))
      (margin, i + 1)
    } :+ (0.0, 0)
    val maxMargin = margins.maxBy(_._1)
    val sum = margins.map(e => math.exp(e._1 - maxMargin._1)).sum
    (1.0 / sum, cat(maxMargin._2))
  }

  private def dot(x: Array[Double], y: Array[Double]): Double = {
    var ret = 0.0
    for (i <- 0 until x.length) {
      ret += x(i) * y(i)
    }
    ret
  }

  private def cos(x: Array[Double], y: Array[Double]): Double = {
    dot(x, y) / (math.sqrt(dot(x, x)) * math.sqrt(dot(y, y)))
  }
}

private object Segment {
  private val SEG = HanLP.newSegment()
  SEG.enableNumberQuantifierRecognize(true)
  SEG.enableCustomDictionary(true)
  SEG.enableNameRecognize(false)

  import scala.collection.JavaConversions._

  def stripHtml(content: String): String = {
    val htmlPatten = "<[^>]+?>"
    if (content == null || content.isEmpty()) ""
    else {
      content.replaceAll("\n"," ").replaceAll("<script>.*?</script>","")
        .replaceAll("(</p>|</br>)\n*", "\n")
        .replaceAll(htmlPatten, " ")
        .replaceAll("(点击加载图片)|(查看原文)|(图片来源)", "")
        .replaceAll("\\s*\n\\s*", "\n")
        .replaceAll("[ \t]+", " ")
        .replaceAll("\n", " ")
    }
  }

  def segment(text: String): Array[String] = {
    if (text == null || text.isEmpty) {
      return Array.empty[String]
    }
    try {
      val t = stripHtml(text).toLowerCase()
      SEG.seg(HanLP.convertToSimplifiedChinese(t))
        .filter(p => (p.nature.startsWith("n") || p.nature.startsWith("mz") || p.nature.equals("mbk")) && p.nature.startsWith("nx") == false) //(p => p.nature.startsWith("n"))
        .map(_.word).toArray
    } catch {
      case e: Exception => Array.empty[String]
    }
  }
}
