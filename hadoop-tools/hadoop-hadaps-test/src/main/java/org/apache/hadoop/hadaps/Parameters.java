/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

class Parameters {

  static enum Mode {
    READ, WRITE
  }

  static final Parameters DEFAULT = new Parameters(
      Mode.READ, "hadaps.testdir", "hadaps.resultdir", "hadaps.csv", 1, 50, 10, 10
  );

  final Mode mode;
  final String inputDirectory;
  final String outputDirectory;
  final String csv;
  final int iteration;
  final int count;
  final int minsize;
  final int maxsize;

  Parameters(Mode mode, String inputDirectory, String outputDirectory, String csv,
      int iteration, int count, int minsize, int maxsize) {
    if (mode == null) throw new IllegalArgumentException();
    if (inputDirectory == null) throw new IllegalArgumentException();
    if (outputDirectory == null) throw new IllegalArgumentException();
    if (csv == null) throw new IllegalArgumentException();
    if (iteration <= 0) throw new IllegalArgumentException();
    if (count <= 0) throw new IllegalArgumentException();
    if (minsize <= 0) throw new IllegalArgumentException();
    if (maxsize <= 0) throw new IllegalArgumentException();

    this.mode = mode;
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
    this.csv = csv;
    this.iteration = iteration;
    this.count = count;
    this.minsize = minsize;
    this.maxsize = maxsize;
  }

}
