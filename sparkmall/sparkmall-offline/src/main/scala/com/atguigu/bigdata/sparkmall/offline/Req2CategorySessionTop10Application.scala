package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.bean.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

// Top10 热门品类中 Top10 活跃 Session 统计
object Req2CategorySessionTop10Application {

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

        // TODO 4.2 配置累加器
        // 创建累加器
        val acc = new CategoryCountAccumulator
        // 注册累加器
        sparkSession.sparkContext.register(acc)

        // TODO 4.3 对原始数据进行遍历，实现累加（聚合）操作
        // 对数据进行累加处理
        userVisitActionRDD.foreach(action=>{
            if ( action.click_category_id != -1 ) {
                // category-click
                // 点击的场合
                acc.add(action.click_category_id + "_click")
            } else {

                if ( action.order_category_ids != null ) {
                    // 下单的场合
                    val ids = action.order_category_ids.split(",")

                    for ( id <- ids ) {
                        acc.add(id + "_order")
                    }
                } else {
                    if (action.pay_category_ids != null) {
                        // 支付的场合
                        val ids = action.pay_category_ids.split(",")

                        for ( id <- ids ) {
                            acc.add(id + "_pay")
                        }
                    }
                }
            }
        })

        // 获取累加器的执行结果
        val statResult : mutable.HashMap[String, Long] = acc.value

        // TODO 4.4 将累加的结果进行分组合并
        // （categoryid, Map( categoryId-clickclickCount, category-orderorderCount, category-paypayCount )）
        val groupMap: Map[String, mutable.HashMap[String, Long]] = statResult.groupBy {
            case (k, v) => { // 1, click
                k.split("_")(0)
            }
        }

        val taskId:String = UUID.randomUUID().toString

        // TODO 4.5 将合并的数据转换为一条数据
        // CategoryTop10(tasked, categoryid, clickcount, ordercount,paycount)
        val dataList: List[CategoryTop10] = groupMap.map {
            case (categoryid, map) => {
                CategoryTop10(
                    taskId,
                    categoryid,
                    map.getOrElse(categoryid + "_click", 0L),
                    map.getOrElse(categoryid + "_order", 0L),
                    map.getOrElse(categoryid + "_pay", 0L))
            }
        }.toList

        // TODO 4.6 将数据排序后取前10条
        val top10Data: List[CategoryTop10] = dataList.sortWith {
            case (left, right) => {
                if (left.clickCount > right.clickCount) {
                    true
                } else if (left.clickCount == right.clickCount) {
                    if (left.orderCount > right.orderCount) {
                        true
                    } else if (left.orderCount == right.orderCount) {
                        left.payCount > right.payCount
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }.take(10)

        //**************************  需求2 代码 ***************************************

        // TODO 4.1 获取前10的品类数据
        // TODO 4.2 将当前的日志数据进行筛选过滤（品类，点击）

        val categoryIds: List[String] = top10Data.map(_.categoryId)
        println(userVisitActionRDD.count())
        val filteredRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
            if (action.click_category_id != -1) {
                /*
                var flg = false
                for (dat <- top10Data) {
                    if ( dat.categoryId == action.click_category_id ) {
                        flg = true
                    }
                }
                flg
                */
                categoryIds.contains("" + action.click_category_id)
            } else {
                false
            }
        })

        println(filteredRDD.count())

        // TODO 4.3 对筛选后的数据进行聚合：
        // (categoryid+sessionidclickCount), (categoryid+sessionidclickCount)
        val categorySessionToCountRDD: RDD[(String, Long)] = filteredRDD.map {
            case action => {
                (action.click_category_id + "_" + action.session_id, 1L)
            }
        }
        // (categoryid+sessionidclickSum), (categoryid+sessionidclickSum)
        val categorySessionToSumRDD: RDD[(String, Long)] = categorySessionToCountRDD.reduceByKey(_+_)
        // TODO 4.4 获取聚合后的数据然后根据品类进行分组
        // (categoryid+sessionidclickCount)转换 (categoryid(sessionid, clickCount))

        val categoryToSessionAndSum: RDD[(String, (String, Long))] = categorySessionToSumRDD.map {
            case (categorySession, sum) => {
                val ks: Array[String] = categorySession.split("_")
                (ks(0), (ks(1), sum))
            }
        }

        val groupRDD: RDD[(String, Iterable[(String, (String, Long))])] = categoryToSessionAndSum.groupBy {
            case (k, v) => {
                k
            }
        }


        /*
        val groupRDD: RDD[(String, Iterable[(String, Long)])] = categorySessionToSumRDD.groupBy {
            case (k, sum) => {
                k.split("_")(0)
            }
        }
        */


        // TODO 4.5 将分组后的数据进行排序（降序），获取前10条数据
        val sortRDD: RDD[(String, List[(String, (String, Long))])] = groupRDD.mapValues(datas => {
            datas.toList.sortWith {
                case (left, right) => {
                    left._2._2 > right._2._2
                }
            }.take(10)
        })
        val mapListRDD: RDD[List[(String, (String, Long))]] = sortRDD.map(_._2)

        val flatMapRDD: RDD[(String, (String, Long))] = mapListRDD.flatMap(x=>x)



        //*****************************************************************

        // TODO 4.7 将结果保存到Mysql中

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

        sparkSession.stop()


    }
}

