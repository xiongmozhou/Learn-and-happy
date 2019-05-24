package com.atguigu.sparkmall.realtime

import java.util
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
object Req4UserBlackListApplication {

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

        val mapDStream: DStream[(String, Int)] = filterDStream.map(message => {

            // 123213213 ==> 2019-04-30
            val dateString: String = SparkmallUtil.parseStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")

            (dateString + "_" + message.userid + "_" + message.adid, 1)
        })

        // 4.3 // 将转换的结果进行聚合 （采集周期内）：（ts_user_ad, sum）
        // TODO 4.4 将不同周期中采集的数据进行累加（有状态）
        // 设定CP的路径
        streamingContext.sparkContext.setCheckpointDir("cp")

        val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
            case (seq, opt) => {
                // 将当前采集周期的统计结果和CP中的数据进行累加
                val sum = seq.sum + opt.getOrElse(0)
                // 将累加的结果放回到CP中
                Option(sum)
            }
        }

        // TODO 4.5 将累加的结果进行判断，是否超过阈值（100）
        stateDStream.foreachRDD(rdd=>{
            rdd.foreach{
                case ( key, sum ) => {
                    if ( sum > 50 ) {
                        // TODO 4.6 如果超过阈值，那么将用户加入黑名单，防止用户继续访问
                        // 将数据更新到redis中
                        //val jedis: Jedis = RedisUtil.getJedisClient
                        val jedis : Jedis = new Jedis("linux4", 6379)

                        // 获取用户
                        val userid = key.split("_")(1)

                        // redis : blackList
                        jedis.sadd("blacklist",userid)

                    }
                }
            }
        })

        streamingContext.start()
        streamingContext.awaitTermination()


    }
}
