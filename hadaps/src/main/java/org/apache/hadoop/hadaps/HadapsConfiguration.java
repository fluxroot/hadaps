/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;

class HadapsConfiguration extends HdfsConfiguration {

  static {
    Configuration.addDefaultResource("hadaps.xml");
  }

}
