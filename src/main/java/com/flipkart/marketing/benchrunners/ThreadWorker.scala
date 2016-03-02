package com.flipkart.marketing.benchrunners

import java.util.Properties

import com.flipkart.marketing.commons.behaviours.Bench
import com.flipkart.marketing.commons.connections.ConnectionProvider
import com.flipkart.marketing.commons.factories.HTableFactoryWrapper
import com.flipkart.marketing.dao.HbaseDao
import com.typesafe.config.ConfigFactory
import com.yammer.metrics.core.{Counter, MetricsRegistry, Timer, TimerContext}
import org.apache.hadoop.hbase.client.{ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 *
 * @author durga.s
 * @version 11/18/15
 */
class ThreadWorker extends HbaseDao with Bench with Runnable{

  var hConnectionHelper = getHBaseConnHelper
  var threadNumber = 0
  var timer: Timer = null
  var exceptionCounter: Counter = null
  var registry: MetricsRegistry = null
  var rowKeys = List("TSRQPLCDEFG_HIJKMNOP")

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
    for (index <- 0 to rowKeys.size)
    {
      val timerR = registry.newTimer(this.getClass, "HBase-Range-Timer")
      val timerIter = registry.newTimer(this.getClass, "HBase-IteratorGet-Timer")
      val timerScan = registry.newTimer(this.getClass, "HBase-Scan-Timer")
      implicit var h = hConnectionHelper.getTableInterface(tblName)

      var ctx: TimerContext = null
      ctx = timerR.time()

      var ctxScan: TimerContext = null
      ctxScan = timerScan.time()
      val scan: Scan = new Scan(Bytes.toBytes(rowKeys(index)), Bytes.toBytes(rowKeys(index)+"~"))
      val rs: ResultScanner = h.getScanner(scan)
      ctxScan.stop()
      val iter = rs.iterator()


      var hasNextValue = iter.hasNext
      var count = 0
      while (hasNextValue && count < 100) {
        var ctxIter: TimerContext = null
        ctxIter = timerIter.time()
        iter.next()
        hasNextValue = iter.hasNext
        ctxIter.stop()
        count += 1
      }
      ctx.stop()
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
