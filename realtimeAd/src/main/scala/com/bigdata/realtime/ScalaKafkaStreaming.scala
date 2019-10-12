package com.bigdata.realtime

import com.bigdata.model.DataModel.MyKafkaMessage
import com.bigdata.util.{CustomDateUtil, CustomKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaKafkaStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ScalaKafkaStream")
      //.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(3))

    //val checkpoint = "/install/huanbgfeng_jars/checkpoint"
    //ssc.sparkContext.setCheckpointDir(checkpoint)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = CustomKafkaUtil.getKafkaStream(ssc)

    val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map(record => {
      val message = record.value()
      val datas: Array[String] = message.split(" ")
      MyKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    val dateAdvUserToCountDStream: DStream[(String, Long)] = messageDStream.map(message => {
      val date: String = CustomDateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
    })

    val stateDStream: DStream[(String, Long)] = dateAdvUserToCountDStream.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }

    val mapDStream: DStream[(String, Long)] = stateDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1) + "_" + keys(3), sum)
      }
    }

    val totalDStream: DStream[(String, Long)] = mapDStream.reduceByKey(_ + _)

    val dateAreaToAdvSumDStream: DStream[(String, (String, Long))] = totalDStream.map {
      case (key, totalSum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1), (keys(2), totalSum))
      }
    }

    val groupDStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToAdvSumDStream.groupByKey()

    val resultDStream: DStream[(String, Map[String, Long])] = groupDStream.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3).toMap
    })

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
