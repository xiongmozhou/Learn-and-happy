package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.bean.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

// 页面单跳转化率统计
object Req3PageFlowApplication {

    def main(args: Array[String]): Unit = {

        // TODO 4.1 获取hive表中的数据
        // 创建配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")

        // 构建SparkSession对象
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        // 引入隐式转换规则
        import sparkSession.implicits._

        val sql : StringBuilder = new StringBuilder("select * from user_visit_action where 1 = 1 ")

        val startDate = SparkmallUtil.getValueFromCondition("startDate")

        if ( SparkmallUtil.isNotEmptyString(startDate) ) {
            sql.append(" and action_time >= '" ).append(startDate).append("' ")
        }

        val endDate = SparkmallUtil.getValueFromCondition("endDate")

        if ( SparkmallUtil.isNotEmptyString(endDate) ) {
            sql.append(" and action_time <= '" ).append(endDate).append("' ")
        }

        // 从hive中查询数据
        sparkSession.sql("use " + SparkmallUtil.getValueFromConfig("hive.database"))
        val dataFrame: DataFrame = sparkSession.sql(sql.toString())

        // 将查询的结果转换为RDD进行统计分析
        val userVisitActionRDD: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd

        //*************************** 需求3 代码 **************************************

        // TODO 获取分母：每一个页面点击次数总和
        // 将日志数据进行过滤，保留需要统计的页面
        val pageid: String = SparkmallUtil.getValueFromCondition("targetPageFlow")
        val pageids: Array[String] = pageid.split(",")

        val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
            pageids.contains("" + action.page_id)
        })

        // 转换结构 action==>(pageid, 1L)
        val pageidToCountRDD: RDD[(Long, Long)] = filterRDD.map(action => {
            (action.page_id, 1L)
        })
        // (1, 100), (2, 50)
        val pageidToSumRDD: RDD[(Long, Long)] = pageidToCountRDD.reduceByKey(_+_)
        val pageidToSumMap: Map[Long, Long] = pageidToSumRDD.collect().toMap
        //pageidToSumRDD.foreach(println)

        // TODO 获取分子：
        // 根据session进行分组
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action=>{action.session_id})

        // 将分组后的数据进行排序
        // session, List( (1-2,1), (2-3,1) )
        val sessionToPageCountListRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas => {
            val sortList: List[UserVisitAction] = datas.toList.sortWith {
                case (left, right) => {
                    left.action_time < right.action_time
                }
            }
            // List(1,2,5,4, 6)
            val pageidList: List[Long] = sortList.map(_.page_id)
            // 拉链 zip
            // (1-2, 1), (2-5,1), (5-4), 4-6
            val pageFlowLost: List[(Long, Long)] = pageidList.zip(pageidList.tail)
            pageFlowLost.map {
                case (before, after) => {
                    (before + "-" + after, 1L)
                }
            }
        })

        // List( (1-2,1), (2-3,1) ) ==> (1-2,1), (2-3,1)
        val pageflowToCountListRDD: RDD[List[(String, Long)]] = sessionToPageCountListRDD.map {
            case (session, list) => {
                list
            }
        }

        // (1-2,1)
        val pageflowToCountRDD: RDD[(String, Long)] = pageflowToCountListRDD.flatMap(x=>x)

        // 将不需要关心页面流转的数据过滤掉
        // 1，2，3，4，5，6，7
        // 1-2，2-3，3-4，4-5，5-6，6-7
        val zipPageids: Array[(String, String)] = pageids.zip(pageids.tail)
        val zipMapPageids: Array[String] = zipPageids.map {
            case (before, after) => {
                before + "-" + after
            }
        }

        val filterZipRDD: RDD[(String, Long)] = pageflowToCountRDD.filter {
            case (pageflow, count) => {
                zipMapPageids.contains(pageflow)
            }
        }

        // (1-2, 100)
        val pageflowToSumRDD: RDD[(String, Long)] = filterZipRDD.reduceByKey(_+_)

        // TODO 页面单跳点击次数 / 页面点击次数
        pageflowToSumRDD.foreach{
            case (pageflow, sum) => {
                val ks: Array[String] = pageflow.split("-")
                // 1-2 / 1
                println( pageflow + "转化率 = " + (sum.toDouble / pageidToSumMap(ks(0).toLong) )  )
            }
        }


        //*****************************************************************

        // TODO 4.7 将结果保存到Mysql中
        /*

        val driverClass = SparkmallUtil.getValueFromConfig("jdbc.driver.class")
        val url = SparkmallUtil.getValueFromConfig("jdbc.url")
        val user = SparkmallUtil.getValueFromConfig("jdbc.user")
        val password = SparkmallUtil.getValueFromConfig("jdbc.password")

        Class.forName(driverClass)


        /*
        flatMapRDD.foreach{
            case ( category, (session, sum) ) => {

                val connection: Connection = DriverManager.getConnection(url, user, password)
                val insertSQL = "insert into category_top10_session_count values ( ?, ?, ?, ?)"
                val statement: PreparedStatement = connection.prepareStatement(insertSQL)

                statement.setObject(1, taskId)
                statement.setObject(2, category)
                statement.setObject(3, session)
                statement.setObject(4, sum)

                statement.executeUpdate()


                statement.close()
                connection.close()
            }
        }
        */
        flatMapRDD.foreachPartition(datas=>{
            val connection: Connection = DriverManager.getConnection(url, user, password)
            val insertSQL = "insert into category_top10_session_count values ( ?, ?, ?, ?)"
            val statement: PreparedStatement = connection.prepareStatement(insertSQL)
            for (( category, (session, sum) )  <- datas) {
                statement.setObject(1, taskId)
                statement.setObject(2, category)
                statement.setObject(3, session)
                statement.setObject(4, sum)

                statement.executeUpdate()
            }
            statement.close()
            connection.close()
        })

*/
        sparkSession.stop()


    }
}

