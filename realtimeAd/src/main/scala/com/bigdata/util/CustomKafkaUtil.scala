package com.bigdata.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object CustomKafkaUtil {

  val bootstrapServers = "10.218.0.71:8092,10.218.0.72:8092,10.218.0.73:8092"
  val groupId = "kafka-test-group"
  val topicName = "fifth"
  val maxPoll = 20000

  val kafkaParams = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    ConsumerConfig.GROUP_ID_CONFIG -> groupId,
    ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  def getKafkaStream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaDStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
    kafkaDStream
  }
}
