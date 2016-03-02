package com.flipkart.marketing.benchrunners

import java.util.{UUID, Properties}

import com.flipkart.marketing.commons.behaviours.Bench
import com.flipkart.marketing.commons.connections.ConnectionProvider
import com.flipkart.marketing.commons.factories.HTableFactoryWrapper
import com.flipkart.marketing.dao.HbaseDao
import com.flipkart.marketing.dao.HbaseDao.RowData
import com.typesafe.config.ConfigFactory
import com.yammer.metrics.core.{TimerContext, Counter, MetricsRegistry, Timer}
import org.apache.commons.lang.CharEncoding

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
  var rowKeys = List("ABCNYTGFEE_HIJKMNOP='ZYXUVW'" , "DEFKRJBTNSRA_EAJKNRSV='ARS'","GHIAEFJKNR_JRKNA='FAEEFA","JKLASDE_AEJKNV='FEAB'","MNORMLKBSAVE_EAVAV='FEAF'","PQRJNGSSLBS_VRSJKK='GNDRE","RSTTKBLSQB_RSBSR='BTDB","UVWKJRSV_RSBJK='ESGGAE'","XYZJRNSBE_BRSJ='BTSSQ'")

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
      implicit var h = hConnectionHelper.getTableInterface(tblName)
      for (i <- 0 to 30000) {
        var ctx: TimerContext = null
        try {
          println("Connection Established FOR thREAD  : " + threadNumber)
          while (true) {
            val rowKeyRand = UUID.randomUUID().toString
            val rowKey = rowKeys(threadNumber-1) + rowKeyRand
            val data: RowData = Map[String, Map[String, Array[Byte]]](
              "d" -> Map[String, Array[Byte]](
                "colq" -> "f".getBytes(CharEncoding.UTF_8)
              )
            )

            ctx = timer.time()

            addRow(rowKey, data)

            ctx.stop()
          }


        } catch {
          case e: Exception =>
            ctx.stop()
            exceptionCounter.inc()
            println(s"E: ${e.getStackTrace}")
        } finally {
          hConnectionHelper.releaseTableInterface(h)
        }
      }
    }


  def getHBaseConnHelper = {

    val configServiceHost = "10.47.0.101"
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

    val zookeeperQuorum = "10.33.17.204,10.33.209.206,10.33.193.227"


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
