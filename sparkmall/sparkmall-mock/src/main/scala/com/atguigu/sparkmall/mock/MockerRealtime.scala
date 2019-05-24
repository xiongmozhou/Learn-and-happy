package com.atguigu.sparkmall.mock

import java.text.SimpleDateFormat
import java.util.{Properties, Random, UUID}

import com.atguigu.bigdata.sparkmall.common.bean.{CityInfo, ProductInfo, UserInfo, UserVisitAction}
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import com.atguigu.sparkmall.mock.util.{RanOpt, RandomDate, RandomNum, RandomOptions}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 生成实时模拟数据
  */
object MockerRealtime {
    def generateMockData(): Array[String] = {
        val array = ArrayBuffer[String]()
        val CityRandomOpt = RandomOptions(RanOpt(CityInfo(1, "北京", "华北"), 30),
            RanOpt(CityInfo(1, "上海", "华东"), 30),
            RanOpt(CityInfo(1, "广州", "华南"), 10),
            RanOpt(CityInfo(1, "深圳", "华南"), 20),
            RanOpt(CityInfo(1, "天津", "华北"), 10))

        val random = new Random()

        for (i <- 0 to new Random().nextInt(10)) {

            val timestamp = System.currentTimeMillis()
            val cityInfo = CityRandomOpt.getRandomOpt()
            val city = cityInfo.city_name
            val area = cityInfo.area
            val adid = 1+random.nextInt(6)
            val userid = 1+random.nextInt(6)

            // 拼接实时数据
            array += timestamp + " " + area + " " + city + " " + userid + " " + adid
        }
        array.toArray

    }

    def createKafkaProducer(broker: String): KafkaProducer[String, String] = {
        // 创建配置对象
        val prop = new Properties()
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        // 根据配置创建Kafka生产者
        new KafkaProducer[String, String](prop)

    }

    def main(args: Array[String]): Unit = {
        val broker = SparkmallUtil.getValueFromConfig("kafka.broker.list")
        val topic =  "ads_log181111"
        // 创建Kafka消费者

        val kafkaProducer = createKafkaProducer(broker)

        while (true) {
            // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
            for (line <- generateMockData()) {
                kafkaProducer.send(new ProducerRecord[String, String](topic, line))
                println(line)

            }
            Thread.sleep(2000)
        }


    }
}
