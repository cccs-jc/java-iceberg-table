package cccs;

import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.util.DateTimeUtil;

public class Writer {

    private Table table;
    private int numRowsPerDataFile = 1;
    private int numDataFilesPerIteration = 1;
    public final Path dataFilesToCommitPath;

    public Writer(int numDataFilesPerIteration, int numRowsPerDataFile) throws Exception {
        this.numDataFilesPerIteration = numDataFilesPerIteration;
        this.numRowsPerDataFile = numRowsPerDataFile;
        this.dataFilesToCommitPath = new Path(Constants.tableLocation, "dataFilesToCommit");
        this.table = Constants.tables.load(Constants.tableLocation);
    }

    public Writer() throws Exception {
        this.dataFilesToCommitPath = new Path(Constants.tableLocation, "dataFilesToCommit");
    }

    public List<Record> createRecords(OffsetDateTime tsTimeperiodLoadedBy) {
        long micros = DateTimeUtil.microsFromTimestamptz(tsTimeperiodLoadedBy);
        GenericRecord record = GenericRecord.create(Constants.SCHEMA);
        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        for (long i = 0; i < numRowsPerDataFile; i++) {
            StringBuffer buffer = new StringBuffer();
            // avg message_body size in kafka is 1432
            // using 35x50=1750 bytes
            // also all data is random so hard to compress
            for (int u = 0; u < 50; u++) {
                buffer.append(UUID.randomUUID().toString());
            }
            OffsetDateTime ts = OffsetDateTime.now();
            builder.add(record.copy(ImmutableMap.of("message_id", i,
                    "data", UUID.randomUUID().toString(),
                    "timestamp", ts,
                    "timeperiod_loadedBy", micros,
                    "message_body", ByteBuffer.wrap(buffer.toString().getBytes()))));
        }
        return builder.build();
    }

    public DataFile createDataFile(String fname, OffsetDateTime tsTimeperiodLoadedBy) throws Exception {
        Transform transform = this.table.spec().getFieldsBySourceId(4).get(0).transform();
        long micros = DateTimeUtil.microsFromTimestamptz(tsTimeperiodLoadedBy);
        PartitionKey loadedByPartition = new PartitionKey(table.spec(), Constants.SCHEMA);
        loadedByPartition.set(0, transform.apply(micros));

        String fullPath = this.table.locationProvider().newDataLocation(this.table.spec(), loadedByPartition, fname);

        OutputFile file = HadoopOutputFile.fromLocation(fullPath, Constants.hadoopConf);

        DataWriter<Record> dataWriter = null;
        if (Constants.parquet) {
            dataWriter = Parquet.writeData(file)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .forTable(this.table)
                    .withPartition(loadedByPartition)
                    .build();
        } else {
            dataWriter = Avro.writeData(file)
                    .forTable(this.table)
                    .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
                    .withPartition(loadedByPartition)
                    .build();
        }

        try {
            for (Record record : createRecords(tsTimeperiodLoadedBy)) {
                dataWriter.write(record);
            }
        } finally {
            dataWriter.close();
        }

        return dataWriter.toDataFile();
    }

    public void loadTable() throws Exception {
        this.table = Constants.tables.load(Constants.tableLocation);
    }

    public void createTable() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("commit.retry.num-retries", "20000");
        properties.put("write.metadata.previous-versions-mmessage_body", "200");
        properties.put("write.metadata.delete-after-commit.enabled", "true");
        properties.put("history.expire.min-snapshots-to-keep", "100");
        properties.put("commit.manifest.min-count-to-merge", "200");

        this.table = Constants.tables.create(Constants.SCHEMA, Constants.partitionSpec, properties,
                Constants.tableLocation);
    }

    public List<DataFile> createDataFiles() throws Exception {
        OffsetDateTime tsTimeperiodLoadedBy = OffsetDateTime.now();
        long start = System.currentTimeMillis();
        List<DataFile> dataFiles = new ArrayList<DataFile>();
        for (int i = 0; i < numDataFilesPerIteration; i++) {
            String uuid = UUID.randomUUID().toString();
            dataFiles.add(createDataFile(uuid + Constants.fileExt, tsTimeperiodLoadedBy));
        }
        long end = System.currentTimeMillis();
        System.out.println("Time to create data files (ms): " + (end - start));
        return dataFiles;
    }

    public long createCommit(List<DataFile> dataFiles) throws Exception {
        long start = System.currentTimeMillis();
        // Using the fast append which does not rewrite existing manifests files but
        // just adds a new one.
        // When we do readStream with spark all we are interested in are the added files
        // in a commit.
        // Having eneficient manifests is not an issue in that case.
        AppendFiles append = this.table.newAppend();
        for (Iterator<DataFile> iter = dataFiles.iterator(); iter.hasNext();) {
            append.appendFile(iter.next());
        }
        append.commit();
        long end = System.currentTimeMillis();
        System.out.println("Time to commit (ms): " + (end - start));
        return (end - start);
    }

    public FileSystem getFileSystem() {
        return Util.getFs(Constants.tableLocationPath, Constants.hadoopConf);
    }

    public void writeDataFilesToCommit(List<DataFile> dataFiles) throws Exception {
        String uuid = UUID.randomUUID().toString();
        FileSystem fs = getFileSystem();
        Path tmpToCommit = new Path(this.dataFilesToCommitPath, uuid + ".ser");
        Path toCommit = new Path(this.dataFilesToCommitPath, "tc_" + uuid + ".ser");
        try (FSDataOutputStream fout = fs.create(tmpToCommit, false /* overwrite */);
                ObjectOutputStream out = new ObjectOutputStream(fout)) {
            out.writeObject(dataFiles);
        }
        fs.rename(tmpToCommit, toCommit);
    }
}
