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

// 广告黑名单实时统计
object Req4RedisUserBlackListApplication {

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

        //messageDStream.print()
        // 将获取的数据进行筛选（过滤掉黑名单的数据）
        /*
        val filterDStream: DStream[KafkaMessage] = messageDStream.filter(message => {
            // 从redis中获取黑名单
            !blackListSet.contains(message.userid)
        })
        */

        val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
            // Driver....
            val jedis: Jedis = RedisUtil.getJedisClient
            val blackListSet: Set[String] = jedis.smembers("blacklist")
            // 广播变量
            val broadcastSet: Broadcast[Set[String]] = streamingContext.sparkContext.broadcast(blackListSet)

            jedis.close()
            rdd.filter(message => {
                // Executor
                //!blackListSet.contains(message.userid)
                !broadcastSet.value.contains(message.userid)
            })
        })

        filterDStream.foreachRDD(rdd=>{

            rdd.foreachPartition(messages=>{
                val jedis: Jedis = RedisUtil.getJedisClient

                for (message <- messages) {
                    val dateString: String = SparkmallUtil.parseStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")
                    //(dateString + "_" + message.userid + "_" + message.adid, 1)
                    // 将redis中指定的key，进行数据累加
                    // key => dateString + "_" + message.userid + "_" + message.adid
                    val key = "date:user:ad:clickcount"
                    val field = dateString + "_" + message.userid + "_" + message.adid

                    jedis.hincrBy(key, field, 1)

                    val sumString: String = jedis.hget(key, field)
                    val sum = sumString.toLong

                    if ( sum >= 100 ) {
                        jedis.sadd("blacklist", message.userid)
                    }
                }

                jedis.close()
            })


        })

        streamingContext.start()
        streamingContext.awaitTermination()


    }
}
