package com.atguigu.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.bean.KafkaMessage
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import com.atguigu.sparkmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

// 最近一小时广告点击趋势
object Req7TimeAdvClickCountTrendApplication {

//    def main1(args: Array[String]): Unit = {
//        var time: String = SparkmallUtil.parseStringDateFromTs(System.currentTimeMillis(), "mm:ss")
//        println((time, 1))
//
//        val prefixTime: String = time.substring(0, 4)
//
//        time = prefixTime + "0"
//
//        println((time, 1))
//    }

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

        /*
        // 将采集周期中的数据进行聚合
        val mapDStream: DStream[(String, Int)] = messageDStream.map(message => {
            val dateString: String = SparkmallUtil.parseStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")
            (dateString + "_" + message.area + "_" + message.city + "_" + message.adid, 1)
        })
        */

        // *************************** 需求 7 *******************************************

        // 7.0 使用窗口函数将多个采集周期的数据作为一个整体进行统计
        val windowDStream: DStream[KafkaMessage] = messageDStream.window(Seconds(60), Seconds(10))

        // 7.1 获取数据，将数据根据窗口的滑动的幅度进行分组

        // 将数据进行结构的转换（KafkaMessage）==> (time,1)
        // （15:15:10, 1), (15:15:12,1), (15:15:21,1)
        //  (15:15:10,1), (15:15:10,1), (15:15:20,1)

        // 15:12 => 15:10
        // 15:20 => 15:20
        // 15:33 => 15:30
        // 15:59 => 15:50
        // 16:00 => 16:00
        val timeToClickDStream: DStream[(String, Int)] = windowDStream.map(message => {

            var time: String = SparkmallUtil.parseStringDateFromTs(message.ts.toLong, "yyyy-MM-dd HH:mm:ss")


            val prefixTime: String = time.substring(0, time.length-1)

            time = prefixTime + "0"

            (time, 1)
        })

        // 7.2 将分组后的数据进行统计聚合
        val timeToSumDStream: DStream[(String, Int)] = timeToClickDStream.reduceByKey(_+_)

        // 7.3 将统计的结果按照时间进行排序
        val sortedDStream: DStream[(String, Int)] = timeToSumDStream.transform(rdd => {
            rdd.sortBy(t => {
                t._1
            }, false)
        })

        sortedDStream.print()
        // 7.4 将排序后的数据保存到redis中

        // **********************************************************************

        // TODO 4.6 将结果保存到redis中
        /*
        resultMapDStream.foreachRDD(rdd=>{
            rdd.foreachPartition(datas=>{
                val jedis: Jedis = RedisUtil.getJedisClient
                for ((k, map) <- datas) {

                    val ks: Array[String] = k.split("_")

                    val key = "top3_ads_per_day:"+ks(0)

                    // 将Scala集合转换为JSON字符串
                    import org.json4s.JsonDSL._
                    val value: String = JsonMethods.compact(JsonMethods.render(map))
                    jedis.hset(key, ks(1), value)
                }

                jedis.close()
            })
        })
*/

        // *****************************************************************

        streamingContext.start()
        streamingContext.awaitTermination()


    }
}
