package com.flipkart.marketing.commons.behaviours

import org.apache.hadoop.hbase.client.HTableInterface

/**
 *
 *
 * @author durga.s
 * @version 11/16/15
 */
trait HTableFactory {
  def getTableInterface(tableName: String): HTableInterface
  def releaseTableInterface(hTableInterface: HTableInterface)
  def shutdown()
}
