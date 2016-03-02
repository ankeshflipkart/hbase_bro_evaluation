package com.flipkart.marketing.commons.connections

import java.util.Properties
import javax.sql.DataSource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HConnection

/**
  * Created by kinshuk.bairagi on 27/01/16.
  */
abstract class TConnectionProvider {


  def createHbaseConnection(hConnConfig: Configuration) : HConnection

  def createDatasourceConnection(mySQLProperties: Properties): DataSource
}
