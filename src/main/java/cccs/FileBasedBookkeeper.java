package cccs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.Util;

public class FileBasedBookkeeper {
    private ExecutorService executorService = new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    private boolean dataFilesToCommitPathChecked = false;
    private final Table table;
    private final Path dataFilesToCommitPath;
    private final Pattern DATAFILE_TO_COMMIT_PATTERN = Pattern.compile("tc_.*\\.ser");

    private long runningCountAppendFile;

    private long cumulativeLatency;

    public class MonikerReader implements Callable<List<DataFile>> {
        private FileStatus s;

        public MonikerReader(FileStatus s) {
            this.s = s;
        }

        public List<DataFile> call() throws Exception {
            return readDataFilesToCommit(s.getPath());
        }
    }

    public class MonikerDeleter implements Callable<Boolean> {
        private final FileStatus f;
        private final FileSystem fs;

        public MonikerDeleter(FileSystem fs, FileStatus f) {
            this.f = f;
            this.fs = fs;
        }

        public Boolean call() throws Exception {
            return fs.delete(f.getPath(), false /* recursive delete */);
        }
    }

    public FileBasedBookkeeper() throws Exception {
        this.dataFilesToCommitPath = new Path(Constants.tableLocation, "dataFilesToCommit");
        this.table = Constants.tables.load(Constants.tableLocation);
    }

    public FileSystem getFileSystem() {
        return Util.getFs(Constants.tableLocationPath, Constants.hadoopConf);
    }

    public List<DataFile> readDataFilesToCommit(Path path) throws Exception {
        try (FSDataInputStream fin = getFileSystem().open(path);
                ObjectInputStream in = new ObjectInputStream(fin)) {
            return (List<DataFile>) in.readObject();
        }
    }

    public FileStatus[] listDataFilesToCommit() throws Exception {
        for (int i = 0; i < 200; i++) {
            try {
                long start = System.currentTimeMillis();
                FileSystem fs = getFileSystem();
                if (!this.dataFilesToCommitPathChecked) {
                    try {
                        if (fs.exists(this.dataFilesToCommitPath) == false) {
                            fs.mkdirs(this.dataFilesToCommitPath);
                        }
                    } catch (IOException io) {
                        System.out.println("failed to create dataFilesToCommitPath.");
                    }
                    this.dataFilesToCommitPathChecked = true;
                }

                FileStatus[] files = fs.listStatus(this.dataFilesToCommitPath,
                        status -> DATAFILE_TO_COMMIT_PATTERN.matcher(status.getName()).matches());
                long end = System.currentTimeMillis();
                System.out.println("Time to list transaction logs (ms): " + (end - start));
                return files;
            } catch (Exception e) {

            }
        }
        return null;
    }

    public void deleteUsedMonikers(FileStatus[] files) throws Exception {
        long start = System.currentTimeMillis();
        FileSystem fs = getFileSystem();
        List<Callable<Boolean>> callableTasks = new ArrayList<>();
        for (FileStatus s : files) {
            callableTasks.add(new MonikerDeleter(fs, s));
        }

        List<Future<Boolean>> futures = executorService.invokeAll(callableTasks);
        futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                return null;
            }
        });
        long end = System.currentTimeMillis();
        System.out.println("Time to delete monikers (ms): " + (end - start));
    }

    public List<DataFile> readMonikers(FileStatus[] files, AppendFiles append) throws Exception {
        long start = System.currentTimeMillis();

        List<Callable<List<DataFile>>> callableTasks = new ArrayList<>();
        for (FileStatus s : files) {
            callableTasks.add(new MonikerReader(s));
        }

        List<Future<List<DataFile>>> futures = executorService.invokeAll(callableTasks);
        List<DataFile> dataFiles = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                return null;
            }
        }).flatMap(Collection::stream).collect(Collectors.toList());

        long end = System.currentTimeMillis();
        System.out.println("Time to read monikers (ms): " + (end - start));
        return dataFiles;
    }

    public long appendAndCommitPendingFiles() throws Exception {
        long start = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        FileStatus[] files = listDataFilesToCommit();
        if (files.length > 0) {
            AppendFiles append = this.table.newAppend();
            for (FileStatus f : files) {
                long mod = f.getModificationTime();
                long latency = (now - mod);
                cumulativeLatency += latency;
            }
            List<DataFile> dataFiles = readMonikers(files, append);
            dataFiles.stream().map(d -> append.appendFile(d));
            runningCountAppendFile += dataFiles.size();
            deleteUsedMonikers(files);
            append.commit();
        }
        long end = System.currentTimeMillis();
        if (files.length > 0) {
            System.out.println(
                    "Total time append " + files.length + " files (ms): "
                            + (end - start) + " Total appended files: " + runningCountAppendFile
                            + " Cumulative latency: " + cumulativeLatency + " Avg latency: "
                            + cumulativeLatency / runningCountAppendFile);
        }
        return (end - start);
    }

    public long markOldDataFileForDelete(int retentionMs) {
        long start = System.currentTimeMillis();
        long deleteMs = (System.currentTimeMillis() - retentionMs);
        long deleteMicros = deleteMs * 1000;
        deleteMicros = deleteMicros - (deleteMicros % Constants.widthMicros);
        System.out.println("deleting data files older than: " + deleteMicros);
        this.table.newDelete().deleteFromRowFilter(Expressions.lessThan("timeperiod_loadedBy", deleteMicros)).commit();
        long end = System.currentTimeMillis();
        System.out.println("Time to delete old data (ms): " + (end - start));
        return (end - start);
    }

}
