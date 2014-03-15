package org.apache.hadoop.hadaps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestMiniCluster {

  @Test
  public void testMiniCluster() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();

    cluster.waitActive();

    cluster.shutdown();
  }

}
