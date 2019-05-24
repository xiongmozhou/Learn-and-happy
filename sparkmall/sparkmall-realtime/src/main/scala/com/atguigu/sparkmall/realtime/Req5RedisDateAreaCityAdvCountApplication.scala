package com.atguigu.sparkmall.realtime

import java.util.Set

import com.atguigu.bigdata.sparkmall.common.bean.KafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import com.atguigu.sparkmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 广告点击量实时统计
object Req5RedisDateAreaCityAdvCountApplication {

    def main(args: Array[String]): Unit = {

        // 创建Spark流式数据处理环境e
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4UserBlackListApplication")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        // TODO 4.1 从kafka中周期性获取广告点击数据
        val topic = "ads_log181111"
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

        // TODO 4.2 将数据进行分解和转换：（ts_user_ad, 1）
        val messageDStream: DStream[KafkaMessage] = kafkaDStream.map(record => {

            val message: String = record.value()
            val datas: Array[String] = message.split(" ")

            KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
        })

        messageDStream.foreachRDD(rdd=>{
            rdd.foreachPartition(messages=>{
                val jedis: Jedis = RedisUtil.getJedisClient
                for (message <- messages) {
                    val dateString: String = SparkmallUtil.parseStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")
                    val key = "date:area:city:ads"
                    val field = dateString + "_" + message.area + "_" + message.city + "_" + message.adid
                    jedis.hincrBy(key, field, 1)
                }
                jedis.close()
            })
        })

        streamingContext.start()
        streamingContext.awaitTermination()


    }
}
