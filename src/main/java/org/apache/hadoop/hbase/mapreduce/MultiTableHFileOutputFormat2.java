package org.apache.hadoop.hbase.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class MultiTableHFileOutputFormat2 extends HFileOutputFormat2 {
    private static final Log LOG = LogFactory.getLog(HFileOutputFormat2.class);

    public static class TableAndRow {
        public String table;
        public byte[] row;
        public TableAndRow(String table, byte[] row) {
            this.table = table;
            this.row = row;
        }
    }

    public static TableAndRow getTableAndRow(ImmutableBytesWritable i_bytes) {
//        Arrays. bytes.get()

        byte[] bytes = i_bytes.get();
        int i = 0;
        for(; i < bytes.length; i++)
            if(bytes[i] == ':')
                break;
        if (i == bytes.length) {
            LOG.error("Output key has an invalid format " + Bytes.toString(bytes));
            throw new IllegalArgumentException("RecordWriter key cannot be split into a table and row");
        } else {
            return new TableAndRow(
                    Bytes.toString(Bytes.head(bytes, i)),
                    Bytes.tail(bytes, bytes.length - i - 1)
            );
        }
    }

    @Override
    public RecordWriter<ImmutableBytesWritable, Cell> getRecordWriter(
            final TaskAttemptContext context) throws IOException, InterruptedException {
        return createRecordWriter(context);
    }

    static <V extends Cell> RecordWriter<ImmutableBytesWritable, V>
    createRecordWriter(final TaskAttemptContext context)
            throws IOException {

        // Get the path of the temporary output file
        final Path outputPath = FileOutputFormat.getOutputPath(context);
        final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();
        final Configuration conf = context.getConfiguration();
        final FileSystem fs = outputdir.getFileSystem(conf);
        // These configs. are from hbase-*.xml
        final long maxsize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
                HConstants.DEFAULT_MAX_FILE_SIZE);
        // Invented config.  Add to hbase-*.xml if other than default compression.
        final String defaultCompressionStr = conf.get("hfile.compression",
                Compression.Algorithm.NONE.getName());
        final Compression.Algorithm defaultCompression = AbstractHFileWriter
                .compressionByName(defaultCompressionStr);
        final boolean compactionExclude = conf.getBoolean(
                "hbase.mapreduce.hfileoutputformat.compaction.exclude", false);

        // create a map from column family to the compression algorithm
        final Map<byte[], Compression.Algorithm> compressionMap = createFamilyCompressionMap(conf);
        final Map<byte[], BloomType> bloomTypeMap = createFamilyBloomTypeMap(conf);
        final Map<byte[], Integer> blockSizeMap = createFamilyBlockSizeMap(conf);

        String dataBlockEncodingStr = conf.get(DATABLOCK_ENCODING_OVERRIDE_CONF_KEY);
        final Map<byte[], DataBlockEncoding> datablockEncodingMap
                = createFamilyDataBlockEncodingMap(conf);
        final DataBlockEncoding overriddenEncoding;
        if (dataBlockEncodingStr != null) {
            overriddenEncoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
        } else {
            overriddenEncoding = null;
        }

        return new RecordWriter<ImmutableBytesWritable, V>() {
            // Map of families to writers and how much has been output on the writer.
            private final Map<byte [], WriterLength> writers =
                    new TreeMap<byte [], WriterLength>(Bytes.BYTES_COMPARATOR);
            private byte [] previousRow = HConstants.EMPTY_BYTE_ARRAY;
            private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
            private boolean rollRequested = false;

            @Override
            public void write(ImmutableBytesWritable row, V cell)
                    throws IOException {
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);

                // null input == user explicitly wants to flush
                if (row == null && kv == null) {
                    rollWriters();
                    return;
                }

                /** CUSTOM CODE HERE */
                TableAndRow tableAndRow = getTableAndRow(row);
                String tableName = tableAndRow.table;
                byte[] rowKey = tableAndRow.row;

                kv = new KeyValue(rowKey,
                        CellUtil.cloneFamily(kv),
                        CellUtil.cloneQualifier(kv),
                        kv.getTimestamp(),
                        CellUtil.cloneValue(kv));
                /** CUSTOM CODE END */

                long length = kv.getLength();
                byte [] family = CellUtil.cloneFamily(kv);
                WriterLength wl = this.writers.get(family);

                // If this is a new column family, verify that the directory exists
                if (wl == null) {
                    /** CUSTOM CODE HERE */
                    Path tableOutputDir = new Path(outputdir, tableName);
                    fs.mkdirs(new Path(tableOutputDir, Bytes.toString(family)));
                    /** CUSTOM CODE END */
                }

                // If any of the HFiles for the column families has reached
                // maxsize, we need to roll all the writers
                if (wl != null && wl.written + length >= maxsize) {
                    this.rollRequested = true;
                }

                // This can only happen once a row is finished though
                if (rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
                    rollWriters();
                }

                // create a new WAL writer, if necessary
                if (wl == null || wl.writer == null) {
                    /** CUSTOM CODE HERE */
                    wl = getNewWriter(tableName, family, conf);
                    /** CUSTOM CODE END */
                }

                // we now have the proper WAL writer. full steam ahead
                kv.updateLatestStamp(this.now);
                wl.writer.append(kv);
                wl.written += length;

                // Copy the row so we know when a row transition.
                this.previousRow = rowKey;
            }

            private void rollWriters() throws IOException {
                for (WriterLength wl : this.writers.values()) {
                    if (wl.writer != null) {
                        LOG.info("Writer=" + wl.writer.getPath() +
                                ((wl.written == 0)? "": ", wrote=" + wl.written));
                        close(wl.writer);
                    }
                    wl.writer = null;
                    wl.written = 0;
                }
                this.rollRequested = false;
            }

            /* Create a new StoreFile.Writer.
             * @param family
             * @return A WriterLength, containing a new StoreFile.Writer.
             * @throws IOException
             */

            private WriterLength getNewWriter(String tableName, byte[] family, Configuration conf)
                    throws IOException {
                WriterLength wl = new WriterLength();

                /** CUSTOM CODE HERE */
                // We need to demux the output per table into separate directories
                Path tableOutputDir = new Path(outputdir, tableName);
                Path familydir = new Path(tableOutputDir, Bytes.toString(family));
                /** CUSTOM CODE END */

                Compression.Algorithm compression = compressionMap.get(family);
                compression = compression == null ? defaultCompression : compression;
                BloomType bloomType = bloomTypeMap.get(family);
                bloomType = bloomType == null ? BloomType.NONE : bloomType;
                Integer blockSize = blockSizeMap.get(family);
                blockSize = blockSize == null ? HConstants.DEFAULT_BLOCKSIZE : blockSize;
                DataBlockEncoding encoding = overriddenEncoding;
                encoding = encoding == null ? datablockEncodingMap.get(family) : encoding;
                encoding = encoding == null ? DataBlockEncoding.NONE : encoding;
                Configuration tempConf = new Configuration(conf);
                tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
                HFileContextBuilder contextBuilder = new HFileContextBuilder()
                        .withCompression(compression)
                        .withChecksumType(HStore.getChecksumType(conf))
                        .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                        .withBlockSize(blockSize);

                if (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
                    contextBuilder.withIncludesTags(true);
                }

                contextBuilder.withDataBlockEncoding(encoding);
                HFileContext hFileContext = contextBuilder.build();

                wl.writer = new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs)
                        .withOutputDir(familydir).withBloomType(bloomType)
                        .withComparator(KeyValue.COMPARATOR)
                        .withFileContext(hFileContext).build();

                this.writers.put(family, wl);
                return wl;
            }

            private void close(final StoreFile.Writer w) throws IOException {
                if (w != null) {
                    w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
                            Bytes.toBytes(System.currentTimeMillis()));
                    w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
                            Bytes.toBytes(context.getTaskAttemptID().toString()));
                    w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
                            Bytes.toBytes(true));
                    w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
                            Bytes.toBytes(compactionExclude));
                    w.appendTrackedTimestampsToMetadata();
                    w.close();
                }
            }

            @Override
            public void close(TaskAttemptContext c)
                    throws IOException, InterruptedException {
                for (WriterLength wl: this.writers.values()) {
                    close(wl.writer);
                }
            }
        };
    }



    /** CUSTOM CODE HERE */
    /**
     * Return the start keys of all of the regions in this table,
     * with the "<table>: " prepended, as a list of ImmutableBytesWritable.
     */
    protected static List<ImmutableBytesWritable> getRegionStartKeys(String tablename, RegionLocator regionLocator)
            throws IOException {
        byte[] keyPrefix = Bytes.toBytes(tablename + ":");
        byte[][] byteKeys = regionLocator.getStartKeys();
        ArrayList<ImmutableBytesWritable> ret =
                new ArrayList<>(byteKeys.length);
        for (byte[] byteKey : byteKeys) {
            // Build <table>:<row>
            byte[] keyWithPrefix = new byte[keyPrefix.length + byteKey.length];
            System.arraycopy(keyPrefix, 0, keyWithPrefix, 0, keyPrefix.length);
            System.arraycopy(byteKey, 0, keyWithPrefix, keyPrefix.length, byteKey.length);
            ret.add(new ImmutableBytesWritable(keyWithPrefix));
        }
        return ret;
    }
    /** END CUSTOM CODE */

    public static void configureIncrementalLoad(Job job, String... tableNames) throws IOException {

        Configuration conf = job.getConfiguration();
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(MultiTableHFileOutputFormat2.class);

        // Based on the configured map output class, set the correct reducer to properly
        // sort the incoming values.
        // TODO it would be nice to pick one or the other of these formats.
        if (KeyValue.class.equals(job.getMapOutputValueClass())) {
            job.setReducerClass(KeyValueSortReducer.class);
        } else if (Put.class.equals(job.getMapOutputValueClass())) {
            job.setReducerClass(PutSortReducer.class);
        } else if (Text.class.equals(job.getMapOutputValueClass())) {
            job.setReducerClass(TextSortReducer.class);
        } else {
            LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
        }

        conf.setStrings("io.serializations", conf.get("io.serializations"),
                MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());


        /** CUSTOM CODE HERE */
        // We need to get the region start keys for each table
        // and generate the sequence file accordingly.
        // writePartitions sorts the start keys later on, so we needn't bother
        int reduceTasks = 0;
        List<ImmutableBytesWritable> allStartKeys = new ArrayList<>();

        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            for (String tablename : tableNames) {
                try (Table table = conn.getTable(TableName.valueOf(tablename))) {

                    // First get the current region splits
                    LOG.info("Looking up current regions for table " + tablename);
                    List<ImmutableBytesWritable> startKeys;
                    try (RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tablename))) {
                        startKeys = getRegionStartKeys(tablename, regionLocator);
                    }
                    allStartKeys.addAll(startKeys);
                    LOG.info("Configuring " + startKeys.size() + " reduce partitions " +
                            "to match current region count");
                    reduceTasks += startKeys.size();

                    // Set compression algorithms based on column families
                    configureCompression(conf, table.getTableDescriptor());
                    configureBloomType(table.getTableDescriptor(), conf);
                    configureBlockSize(table.getTableDescriptor(), conf);
                    configureDataBlockEncoding(table.getTableDescriptor(), conf);
                }
            }
        }


        TreeSet<ImmutableBytesWritable> sorted =
                new TreeSet<>(allStartKeys);
        sorted.first().set(new byte[0]);
        allStartKeys = new ArrayList<>(sorted);

        job.setNumReduceTasks(reduceTasks);
        /** END CUSTOM CODE */

        configurePartitioner(job, allStartKeys);

        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initCredentials(job);

        LOG.info("Incremental table output configured.");
    }
}
