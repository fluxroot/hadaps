/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.util.concurrent.TimeUnit;

class Utils {

  private Utils() {
  }

  static String getPrettyTime(long duration) {
    assert duration > 0;

    return String.format(
      "%02d:%02d:%02d.%03d",
      TimeUnit.MILLISECONDS.toHours(duration),
      TimeUnit.MILLISECONDS.toMinutes(duration)
          - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(duration)),
      TimeUnit.MILLISECONDS.toSeconds(duration)
          - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration)),
      duration - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(duration)));
  }

  static String getHexString(byte[] bytes) {
    return new HexBinaryAdapter().marshal(bytes);
  }

}
