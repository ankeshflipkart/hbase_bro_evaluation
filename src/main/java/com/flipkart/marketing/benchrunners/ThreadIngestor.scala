package com.flipkart.marketing.benchrunners

import java.util.Properties

import com.flipkart.marketing.commons.behaviours.Bench
import com.flipkart.marketing.commons.connections.ConnectionProvider
import com.flipkart.marketing.commons.factories.HTableFactoryWrapper
import com.flipkart.marketing.dao.HbaseDao
import com.typesafe.config.ConfigFactory
import com.yammer.metrics.core.{Counter, MetricsRegistry, Timer}

/**
 *
 *
 * @author durga.s
 * @version 11/18/15
 */
class ThreadIngestor extends HbaseDao with Bench with Runnable{

  var hConnectionHelper = getHBaseConnHelper
  var threadNumber = 0
  var timer: Timer = null
  var exceptionCounter: Counter = null
  var registry: MetricsRegistry = null
  var rowKeys = List("ABCNYTGFEE_HIJKMNOP='ZYXUVW'" , "DEFNKRJBTNSRA_EAJKNRSV='ARS'","GHINAEFJKNR_JRKNA='FAEEFA","JKLNASDE_AEJKNV='FEAB'","MOPNRMLKBSAVE_EAVAV='FEAF'","PQRNJNGSSLBS_VRSJKK='GNDRE","RSTNTKBLSQB_RSBSR='BTDB","UVWNJRSV_RSBJK='ESGGAE'","XYZNJRNSBE_BRSJ='BTSSQ'","OKLNYTGFEE_HIJKMNOP='ZYXUVW'")

  def  this( hConnection: HTableFactoryWrapper , threadNumber : Int, _timer: Timer, _eCounter: Counter, _registry : MetricsRegistry) {
    this()
    this.hConnectionHelper = hConnection
    this.threadNumber = threadNumber
    this.timer = _timer
    this.exceptionCounter = _eCounter
    this.registry = _registry
  }
  val tblName = "cross_pivot_instance_test"


    override def run(): Unit = {
//      implicit var h = hConnectionHelper.getTableInterface(tblName)
//      for (i <- 0 to 30000) {
//        var ctx: TimerContext = null
//        try {
//          val rowKeyRand = UUID.randomUUID().toString
//          val rowKey = rowKeys(i%10).replace("N",threadNumber.toString) + rowKeyRand
//          val data: RowData = Map[String, Map[String, Array[Byte]]](
//            "d" -> Map[String, Array[Byte]](
//              "colq" -> "f".getBytes(CharEncoding.UTF_8)
//            )
//          )
//
//          ctx = timer.time()
//          addRow(rowKey, data)
//
//          ctx.stop()
//
//        } catch {
//          case e: Exception =>
//            ctx.stop()
//            exceptionCounter.inc()
//            println(s"E: ${e.getStackTrace}")
//        } finally {
//          hConnectionHelper.releaseTableInterface(h)
//        }
//      }
    }


  def getHBaseConnHelper = {

    val configServiceHost = ""
    val configServicePort = "80"
    val hConfProps = new Properties()
    /*
        try {
          (hConfProps.setProperty("config.host" , configServiceHost), hConfProps.setProperty("config.port",configServicePort))
        } catch {
          case e: Exception =>
            println("Unable to Connect - " + e.printStackTrace())
        }
    */

    val zookeeperQuorum = ""


    hConfProps.setProperty("hbase.zookeeper.quorum", zookeeperQuorum)
    hConfProps.setProperty("hbase.zookeeper.property.clientPort", "2181")

    val hConfig = ConfigFactory.parseProperties(hConfProps)

//    println("connection : " + hConfig)

    new HTableFactoryWrapper(hConfig, new ConnectionProvider)
  }

  override def beforeAll(): Unit = ???

  override def afterAll(): Unit = ???

  override def runBench(): Unit = ???
}
