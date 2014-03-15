/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

class Csv implements Closeable {

  private final BufferedWriter csv;

  Csv(String filename) throws IOException {
    if (filename == null) throw new IllegalArgumentException();

    Path file = Paths.get(filename);
    csv = Files.newBufferedWriter(file, Charset.defaultCharset());
  }

  void write(List<Statistic> statistics) throws IOException {
    if (statistics == null) throw new IllegalArgumentException();

    Collections.sort(statistics);

    long averageDuration = 0;
    Statistic prevStatistic = null;
    for (Statistic statistic : statistics) {
      if (prevStatistic == null) {
        prevStatistic = statistic;
      }

      // Write average of iteration
      if (!statistic.getFilename().equals(prevStatistic.getFilename())) {
        write(new Statistic(
            0,
            prevStatistic.getFilename(),
            prevStatistic.getReplication(),
            prevStatistic.getSize(),
            averageDuration));

        averageDuration = 0;
      }

      write(statistic);

      averageDuration += statistic.getDuration();
      prevStatistic = statistic;
    }

    // Write average of last iteration
    if (prevStatistic != null) {
      write(new Statistic(
          0,
          prevStatistic.getFilename(),
          prevStatistic.getReplication(),
          prevStatistic.getSize(),
          averageDuration));
    }
  }

  private void write(Statistic statistic) throws IOException {
    assert statistic != null;

    csv.write(Integer.toString(statistic.getIteration()));
    csv.write(";");
    csv.write(statistic.getFilename());
    csv.write(";");
    csv.write(Short.toString(statistic.getReplication()));
    csv.write(";");
    csv.write(Long.toString(statistic.getSize()));
    csv.write(";");
    csv.write(Utils.getPrettyTime(statistic.getDuration()));
    csv.newLine();
  }

  @Override
  public void close() throws IOException {
    csv.close();
  }

}
