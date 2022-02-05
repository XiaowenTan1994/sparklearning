package core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile(WordCount.getClass.getResource("/WordCountInput.txt").toString)

    val words: RDD[String] = lines.flatMap(_.split(" "))

    /**
     * method 3
     */

    val wordCounts = words.map(word => {
      (word, 1)
    }).reduceByKey(_+_)



    /**
     * method 2
     */
    //    val wordToOne = words.map(word => {
    //      (word, 1)
    //    })
    //
    //    val wordGroup: RDD[(String, Iterable[(String,Int)])] = wordToOne.groupBy(wordUnit => wordUnit._1)
    //
    //    val wordCounts = wordGroup.map{
    //      case (word, list) =>{
    //        list.reduce((t1, t2) => {
    //          (t1._1, t1._2 + t2._2)
    //        })
    //      }
    //    }


    /**
     * method 1
     */

    //    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //
    //    val wordCounts = wordGroup.map(dataUnit => {
    //      (dataUnit._1, dataUnit._2.size)
    //    })
    //
    wordCounts.foreach(dataUnit => {
      println(dataUnit._1 + " " + dataUnit._2)
    })

    sc.stop()
  }
}
