package cccs;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;

public class Main {

  public void sleep(long tookMs, long sleepMs) throws Exception {
    long actual = sleepMs - tookMs;
    if (actual > 0) {
      Thread.sleep(actual);
    }
  }

  public void run(String[] args) throws Exception {
    if (args.length > 0) {
      if ("clean".equals(args[0])) {
        FileUtils.deleteDirectory(new File(Constants.destination));
      } else if ("create".equals(args[0])) {
        Writer writer = new Writer();
        writer.createTable();
        System.out.println("table created");
      } else if ("writeandcommitfiles".equals(args[0])) {
        int numIterations = Integer.parseInt(args[1]);
        int numDataFilesPerIteration = Integer.parseInt(args[2]);
        int numRowsPerDataFile = Integer.parseInt(args[3]);
        Writer writer = new Writer(numDataFilesPerIteration, numRowsPerDataFile);
        for (int c = 0; c < numIterations; c++) {
          List<DataFile> dataFiles = writer.createDataFiles();
          writer.createCommit(dataFiles);
        }
      } else if ("writers".equals(args[0])) {
        int numIterations = Integer.parseInt(args[1]);
        int numDataFilesPerIteration = Integer.parseInt(args[2]);
        int numRowsPerDataFile = Integer.parseInt(args[3]);
        Writer writer = new Writer(numDataFilesPerIteration, numRowsPerDataFile);
        for (int c = 0; c < numIterations; c++) {
          List<DataFile> dataFiles = writer.createDataFiles();
          writer.writeDataFilesToCommit(dataFiles);
        }
      } else if ("writers2".equals(args[0])) {
        int numIterations = Integer.parseInt(args[1]);
        int numDataFilesPerIteration = Integer.parseInt(args[2]);
        int numRowsPerDataFile = Integer.parseInt(args[3]);
        Writer writer = new Writer(numDataFilesPerIteration, numRowsPerDataFile);
        for (int c = 0; c < numIterations; c++) {
          writer.createDataFiles();
        }
      } else if ("bookkeeper".equals(args[0])) {
        int sleepMs = Integer.parseInt(args[1]);
        int retentionMs = Integer.parseInt(args[2]);
        int markOldIntervalMs = Integer.parseInt(args[3]);
        long lastMark = System.currentTimeMillis();
        FileBasedBookkeeper bookkeeper = new FileBasedBookkeeper();
        while (true) {
          System.out.println("--------- mark new data file for append --------------");
          long tookMs = bookkeeper.appendAndCommitPendingFiles();
          if (System.currentTimeMillis() > lastMark + markOldIntervalMs) {
            lastMark = System.currentTimeMillis();
            System.out.println("--------- mark old data file for delete files older than " + (retentionMs / 1000 / 60)
                + " minutes --------------");
            long del = bookkeeper.markOldDataFileForDelete(retentionMs);
            tookMs = tookMs + del;
          }
          sleep(tookMs, sleepMs);
        }
      } else if ("bookkeeper2".equals(args[0])) {
        int sleepMs = Integer.parseInt(args[1]);
        int retentionMs = Integer.parseInt(args[2]);
        int markOldIntervalMs = Integer.parseInt(args[3]);
        long lastMark = System.currentTimeMillis();
        StorageQueueBasedBookkeeper bookkeeper = new StorageQueueBasedBookkeeper();
        while (true) {
          // System.out.println("--------- mark new data file for append --------------");
          long tookMs = bookkeeper.processStorageQueue();
          if (System.currentTimeMillis() > lastMark + markOldIntervalMs) {
            lastMark = System.currentTimeMillis();
            System.out.println("--------- mark old data file for delete files older than" + (retentionMs / 1000 / 60)
                + " minutes --------------");
            long del = bookkeeper.markOldDataFileForDelete(retentionMs);
            tookMs = tookMs + del;
          }
          sleep(tookMs, sleepMs);
        }
      } else if ("reaper".equals(args[0])) {
        Reaper reaper = new Reaper();
        int sleepMs = Integer.parseInt(args[1]);
        while (true) {
          System.out.println(
              "--------- expire old snapshots removing metadata and data files which are no longer referenced --------------");
          long tookMs = reaper.expireSnapshots();
          sleep(tookMs, sleepMs);
        }
      } else {
        System.out.println("invalid argument");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new Main().run(args);
  }

}