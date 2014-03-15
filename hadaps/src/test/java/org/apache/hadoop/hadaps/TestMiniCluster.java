package org.apache.hadoop.hadaps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;

public class TestMiniCluster {

  private static final String DATA = "Test Data";
  
  @Test
  public void testMiniCluster() throws IOException {
    // Setup
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
    cluster.waitActive();
    
    try {
      FileSystem fs = cluster.getFileSystem();
      Path file = new Path("/test.txt");
      
      // Write a file
      FSDataOutputStream out = fs.create(file);
      try {
        out.writeUTF(DATA);
      } finally {
        out.close();
      }
      
      // Read the file
      FSDataInputStream in = fs.open(file);
      String data = null;
      try {
        data = in.readUTF();
      } finally {
        in.close();
      }
      
      // Compare the data
      Assert.assertEquals(DATA, data);
    } finally {
      cluster.shutdown();
    }
  }

}
