package com.atguigu.bigdata.sparkmall.offline

import com.atguigu.bigdata.sparkmall.common.bean.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

// 网站页面平均停留时长
object Req8PageAvgAccessTimeApplication {

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

        //*************************** 需求8 代码 **************************************

        // 8.1 获取离线数据
        // 8.2 将数据根据session进行分组
        val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action => {
            action.session_id
        })

        // 8.3 将分组后的数据按照访问时间进行排序
        val sessionToPageidTimeRDD: RDD[(String, List[(Long, Long)])] = sessionGroupRDD.mapValues(datas => {
            val actions: List[UserVisitAction] = datas.toList.sortWith {
                case (left, right) => {
                    left.action_time < right.action_time
                }
            }
            // (A-time),(B-time),(C-time),(D,time)
            val pageidToTimeList: List[(Long, String)] = actions.map(action => {
                (action.page_id, action.action_time)
            })
            // ==>
            // ((A-time), (B-time)), ((B-time), (C-time)), ((C-time), (D,time))
            val zipList: List[((Long, String), (Long, String))] = pageidToTimeList.zip(pageidToTimeList.tail)
            // map ==>
            //  (A, time), (B, time), (C, time)
            zipList.map {
                case (before, after) => {

                    // 时间差
                    val startTime = SparkmallUtil.formatDateFromString(before._2).getTime
                    val endTime = SparkmallUtil.formatDateFromString(after._2).getTime


                    (before._1, (endTime - startTime))
                }
            }
        })
        // 8.4 采用拉链的方式将数据进行组合，形成页面跳转路径（A=>B,B=>C）
        val listRDD: RDD[List[(Long, Long)]] = sessionToPageidTimeRDD.map {
            case (session, list) => {
                list
            }
        }

        // 将集合数据进行扁平化操作
        val pageidToTimeRDD: RDD[(Long, Long)] = listRDD.flatMap(x=>x)

        // 8.5 对跳转路径中的第一个页面进行分组，聚合数据
        val pageidToTimeListRDD: RDD[(Long, Iterable[Long])] = pageidToTimeRDD.groupByKey()

        val pageidToAvgTimeRDD: RDD[(Long, Long)] = pageidToTimeListRDD.mapValues(datas => {
            datas.sum / datas.size
        })
        // 8.6 对聚合后的数据进行计算，获取结果
        pageidToAvgTimeRDD.foreach(println)


        //****************************************************************************
        sparkSession.stop()


    }
}

