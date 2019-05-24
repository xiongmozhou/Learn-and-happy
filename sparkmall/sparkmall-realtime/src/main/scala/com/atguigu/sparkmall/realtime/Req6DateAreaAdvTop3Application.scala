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

// 每天各地区 top3 热门广告
object Req6DateAreaAdvTop3Application {

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

        // 将采集周期中的数据进行聚合
        val mapDStream: DStream[(String, Int)] = messageDStream.map(message => {
            val dateString: String = SparkmallUtil.parseStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")
            (dateString + "_" + message.area + "_" + message.city + "_" + message.adid, 1)
        })

        streamingContext.sparkContext.setCheckpointDir("cp")

        val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
            case (seq, opt) => {
                val sum = seq.sum + opt.getOrElse(0)
                Option(sum)
            }
        }

        // *************************** 需求6 **************************************
        // TODO 4.1 获取需求5的数据
        // TODO 4.2 将获取的数据进行格式转换（date_area_city_adv,sum）(date_area_adv,sum)
        val dateAreaAdvToSumDStream: DStream[(String, Int)] = stateDStream.map {
            case (key, sum) => {

                val keys: Array[String] = key.split("_")

                val newKey = keys(0) + "_" + keys(1) + "_" + keys(3)

                (newKey, sum)
            }
        }

        // TODO 4.3 将转换的数据进行聚合统计：(date_area_adv,sum) (date_area_adv,totalSum)
        val dateAreaAdvToTotalSumDStream: DStream[(String, Int)] = dateAreaAdvToSumDStream.reduceByKey(_+_)

        // TODO 4.4 将统计的结果进行结构转换：(date_area_adv,totalSum)( date_area, ( adv, totalSum ) )
        val dateAreaToAdvTotalSumDStream: DStream[(String, (String, Int))] = dateAreaAdvToTotalSumDStream.map {
            case (key, totalSum) => {
                val keys: Array[String] = key.split("_")
                ((keys(0) + "_" + keys(1)), (keys(2), totalSum))
            }
        }

        // TODO 4.5 将转换后的数据进行分组排序：（  date_area，Map(  adv1totalSum1, adv2totalSum2 ) ）
        val groupDStream: DStream[(String, Iterable[(String, Int)])] = dateAreaToAdvTotalSumDStream.groupByKey()
        val resultMapDStream: DStream[(String, Map[String, Int])] = groupDStream.mapValues(datas => {
            // 对数据进行排序，获取排序后数据的前三名
            datas.toList.sortWith {
                case (left, right) => {
                    left._2 > right._2
                }
            }.take(3).toMap
        })

        // TODO 4.6 将结果保存到redis中
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


        // *****************************************************************

        streamingContext.start()
        streamingContext.awaitTermination()


    }
}
