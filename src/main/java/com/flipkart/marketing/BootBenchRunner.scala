package com.flipkart.marketing

import com.flipkart.marketing.benchrunner.HbaseDaoTest

/**
 * Created by ankesh.maheshwari on 24/02/16.
 */
object BootBenchRunner {

  def main(args: Array[String]) = {
//    val hbaseBench: Bench =

    val conn : HbaseDaoTest = new HbaseDaoTest

//    conn.rangeScan(new MetricsRegistry())
    conn.runBench()
//    conn.runIngestor()
  }

}
