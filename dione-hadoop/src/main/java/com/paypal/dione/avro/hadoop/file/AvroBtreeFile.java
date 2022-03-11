package com.paypal.dione.avro.hadoop.file;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A BtreeKeyValueFile is a b-tree indexed Avro container file of KeyValue records.
 * <p>
 * it is basically a copy from org.apache.avro.hadoop.file.SortedKeyValueFile with changes so every block in the avro
 * file is a b-tree node, and every row has one additional "long" field that points to this record's child node if
 * if is not a leaf record
 */
public class AvroBtreeFile {
    public static final String DATA_SIZE_KEY = "data_bytes";
    public static final String METADATA_COL_NAME = "metadata";
    public static final String KEY_VALUE_HEADER_NAME = "btree.spec.kv";
    private static final Logger logger = LoggerFactory.getLogger(AvroBtreeFile.class);

    // Schema of Long that can be null
    public static Schema metadataSchema = SchemaBuilder.unionOf().nullType().and().longType().endUnion();

    public static class Reader implements Closeable {
        private final Long dataSize;
        private final long fileHeaderEnd;

        private final DataFileReader<GenericRecord> mFileReader;

        private final Schema mKeySchema;
        private final Schema mValueSchema;

        public Schema getKeySchema() {
            return mKeySchema;
        }

        public Schema getValueSchema() {
            return mValueSchema;
        }

        /**
         * A class to encapsulate the options of a Reader.
         */
        public static class Options {
            private Configuration mConf;

            private Path mPath;

            public Options withConfiguration(Configuration conf) {
                mConf = conf;
                return this;
            }

            public Configuration getConfiguration() {
                return mConf;
            }

            public Options withPath(Path path) {
                mPath = path;
                return this;
            }

            public Path getPath() {
                return mPath;
            }

        }

        public Reader(Options options) throws IOException {
            // Open the data file.
            Path dataFilePath = options.getPath();
            logger.debug("Loading the data file " + dataFilePath);
            DatumReader<GenericRecord> datumReader = GenericData.get().createDatumReader(null);
            mFileReader = new DataFileReader<>(new FsInput(dataFilePath, options.getConfiguration()), datumReader);
            String[] split = mFileReader.getMetaString(KEY_VALUE_HEADER_NAME).split("\\|");
            mKeySchema = projectSchema(mFileReader.getSchema(), split[0].split(","));
            mValueSchema = projectSchema(mFileReader.getSchema(), split[1].split(","));

            fileHeaderEnd = mFileReader.previousSync();
            dataSize = mFileReader.getMetaLong(DATA_SIZE_KEY);
        }

        // TODO: do we need this sync?
        public void sync(Long syncPosition) throws IOException {
            mFileReader.sync(syncPosition);
        }

        /**
         * This is the main motivation function of this class.
         * Given a key, run on the records in a "b-tree" manner - to fetch the correct value, if exists,
         * with minimal number of hops between different position in the file.
         * idea is that randomly seeking to a specific position is much more expensive than reading many records
         * sequentially.
         */
        public Iterator<GenericRecord> get(GenericRecord key) {
            logger.debug("searching for key: {}", key);
            return new Iterator<GenericRecord>() {

                long curOffset;
                GenericRecord lastRecord = null;
                int counter;
                long blockCount;
                private RecordProjection projection = new RecordProjection(mKeySchema, mValueSchema);

                GenericRecord nxt = getNextFromOffset(0);

                @Override
                public boolean hasNext() {
                    return nxt!=null;
                }

                @Override
                public GenericRecord next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    GenericRecord ret = nxt;
                    nxt = getNext();
                    return ret;
                }

                private GenericRecord getNextFromOffset(long offset) {
                    curOffset = offset;
                    init();
                    return getNext();
                }

                private void init() {
                    curOffset += fileHeaderEnd;
                    logger.debug("seeking to position: " + curOffset);
                    counter = 0;
                    blockCount = -1;
                    lastRecord = null;
                    try {
                        mFileReader.seek(curOffset);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    mFileReader.hasNext();
                }

                private GenericRecord getNext() {
                    while (mFileReader.hasNext() && (counter < blockCount || blockCount < 0)) {
                        GenericRecord record = mFileReader.next();
                        if (blockCount < 0) blockCount = mFileReader.getBlockCount();

                        counter += 1;
                        int comparison = GenericData.get().compare(projection.getKey(record), key, mKeySchema);
                        logger.debug("comparison was: {} with: {} and {}", comparison, projection.getKey(record), key);
                        if (0 == comparison) {
                            // We've found it!
                            logger.debug("Found record for key {}", key);
                            lastRecord = record;
                            return projection.getValue(record);
                        } else if (comparison > 0) {
                            // We've passed it.
                            if (lastRecord == null || projection.getMetadata(lastRecord) == null) {
                                logger.debug("key does not appear in the file: {}", key);
                                curOffset -= fileHeaderEnd;
                                return null;
                            } else {
                                return getNextFromOffset(getRealOffset(lastRecord));
                            }
                        }
                        lastRecord = record;
                    }
                    if (lastRecord != null && projection.getMetadata(lastRecord) != null) {
                        return getNextFromOffset(getRealOffset(lastRecord));
                    }

                    logger.debug("reached end of road. key does not appear in the file: {}", key);
                    return null;
                }

            };
        }

        private Long getRealOffset(GenericRecord record) {
            Long offset = dataSize;
            Long reversedOffset = (Long) record.get(METADATA_COL_NAME);
            if (reversedOffset != null)
                offset -= reversedOffset;
            return offset;
        }

        /**
         * this iterator runs on the records in sorted order, and not in the "b-tree" order the records are
         * saved in the file
         */
        public Iterator<GenericRecord> getIterator() {
            return new Iterator<GenericRecord>() {

                private final RecordProjection projection = new RecordProjection(mKeySchema, mValueSchema);
                private Node next = new Node(0);

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public GenericRecord next() {
                    if(!hasNext()) throw new NoSuchElementException();
                    GenericRecord ret = next.getCurGenericRecord();
                    if (next.curHasChild()) {
                        next = next.getChildNode();
                    } else {
                        next.curRecord++;
                        // in case we got to the last record
                        while (next.curRecord == next.records.size()) {
                            next = next.parent;
                            if (next == null)
                                return ret;
                            next.curRecord++;
                        }
                    }
                    return ret;
                }

                class Node {
                    Node(long offset) {
                        try {
                            mFileReader.seek(fileHeaderEnd + offset);
                            GenericRecord firstRecord = mFileReader.next();
                            // we only know the block count after the first next()
                            int blockCount = (int) mFileReader.getBlockCount();
                            records = new ArrayList<>(blockCount);
                            records.add(firstRecord);
                            for (int i=1; i<blockCount; i++) {
                                records.add(mFileReader.next());
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    boolean curHasChild() {
                        return projection.getMetadata(records.get(curRecord)) != null;
                    }

                    Node getChildNode() {
                        Node childNode = new Node(getRealOffset(records.get(curRecord)));
                        childNode.parent = this;
                        return childNode;
                    }

                    GenericRecord getCurGenericRecord() {
                        return records.get(curRecord);
                    }

                    int curRecord = 0;
                    final List<GenericRecord> records;
                    Node parent;
                }

            };
        }

        @Override
        public void close() throws IOException {
            mFileReader.close();
        }
    }

    /**
     * Writes a SortedKeyValueFile.
     */
    public static class Writer implements Closeable {
        private final Schema mKeySchema;
        private final Schema mValueSchema;

        private final Schema mRecordSchema;

        private GenericData model;

        private final BufferedWriter bufferedWriter;

        private FileSystem fileSystem;
        private final Path filename;

        private final int mInterval;
        private final int mHeight;

        private GenericRecord mPreviousKey;
        private Node curNode = new Node();
        private final Node root = curNode;

        private class Node {
            List<GenericRecord> records;
            Node prev;
            int height;

            public Node() {
                records = new ArrayList<>(mInterval);
            }

            public Node(Node prevNode) {
                records = new ArrayList<>(mInterval);
                prev = prevNode;
                height = prevNode.height + 1;
            }

            public GenericRecord getCurRecord() {
                return records.get(records.size() - 1);
            }

            public void addRecord(GenericRecord record) throws IOException {
                records.add(record);
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                for (GenericRecord md : records) {
                    sb.append("\n\t\t");
                    sb.append(" data: " + md.toString());
                }
                return "Node{" +
                        "records=" + sb +
                        ", height=" + height +
                        '}';
            }
        }

        /**
         * A class to encapsulate the various options of a Writer.
         */
        public static class Options {
            private Schema mKeySchema;
            private Schema mValueSchema;

            private Configuration mConf;

            private Path mPath;

            private int mInterval = 128;
            private int mHeight = 2;
            private int initialCapacityMB = 20;

            private GenericData model = SpecificData.get();

            private CodecFactory codec = CodecFactory.nullCodec();

            public Options withKeySchema(Schema keySchema) {
                mKeySchema = keySchema;
                return this;
            }

            public Schema getKeySchema() {
                return mKeySchema;
            }

            public Options withValueSchema(Schema valueSchema) {
                mValueSchema = valueSchema;
                return this;
            }

            public Schema getValueSchema() {
                return mValueSchema;
            }

            public Options withConfiguration(Configuration conf) {
                mConf = conf;
                return this;
            }

            public Configuration getConfiguration() {
                return mConf;
            }

            public Options withPath(Path path) {
                mPath = path;
                return this;
            }

            public Path getPath() {
                return mPath;
            }

            public Options withInterval(int interval) {
                mInterval = interval;
                return this;
            }

            public int getInterval() {
                return mInterval;
            }

            public Options withHeight(int height) {
                mHeight = height;
                return this;
            }

            public int getHeight() {
                return mHeight;
            }

            public Options withDataModel(GenericData model) {
                this.model = model;
                return this;
            }

            public GenericData getDataModel() {
                return model;
            }

            public Options withCodec(String codec) {
                this.codec = CodecFactory.fromString(codec);
                return this;
            }

            public Options withCodec(CodecFactory codec) {
                this.codec = codec;
                return this;
            }

            public Options withInitialBufferSizeMB(int mb) {
                this.initialCapacityMB = mb;
                return this;
            }

            public CodecFactory getCodec() {
                return this.codec;
            }
        }

        /**
         * Creates a writer for a new file.
         *
         * @param options The options.
         * @throws IOException If there is an error.
         */
        public Writer(Options options) throws IOException {
            this.model = options.getDataModel();

            if (null == options.getConfiguration()) {
                throw new IllegalArgumentException("Configuration may not be null");
            }

            fileSystem = options.getPath().getFileSystem(options.getConfiguration());
            filename = options.getPath();

            // Save the key and value schemas.
            mKeySchema = options.getKeySchema();
            if (null == mKeySchema) {
                throw new IllegalArgumentException("Key schema may not be null");
            }
            mValueSchema = options.getValueSchema();
            if (null == mValueSchema) {
                throw new IllegalArgumentException("Value schema may not be null");
            }

            mInterval = options.getInterval();
            mHeight = options.getHeight() - 1;
            if (mHeight < 0)
                throw new RuntimeException("Height must be positive, given: " + options.getHeight());

            // Create the parent directory.
            if (!fileSystem.mkdirs(options.getPath().getParent())) {
                throw new IOException(
                        "Unable to create directory: " + options.getPath().getParent());
            }
            logger.debug("Created directory " + options.getPath());

            // Open a writer for the data file.
            Path dataFilePath = options.getPath();
            logger.debug("Creating writer for avro data file: " + dataFilePath);
            List<Schema.Field> schemaFields = new ArrayList<>();
            mRecordSchema = createSchema(mKeySchema, mValueSchema);
            String keys = String.join(",", mKeySchema.getFields().stream().map(Schema.Field::name).toArray(String[]::new));
            String values = String.join(",",  mValueSchema.getFields().stream().map(Schema.Field::name).toArray(String[]::new));
            String keyValueFields = keys + "|" + values;
            bufferedWriter = new BufferedWriter(options, mRecordSchema, keyValueFields);
        }

        /**
         * TODO: add doc
         */
        public void append(GenericRecord key, GenericRecord value) throws IOException {
            // Make sure the keys are inserted in sorted order.
            if (null != mPreviousKey && model.compare(key, mPreviousKey, mKeySchema) < 0) {
                throw new IllegalArgumentException("Records must be inserted in sorted key order."
                        + " Attempted to insert key " + key + " after " + mPreviousKey + ".");
            }
            mPreviousKey = model.deepCopy(mKeySchema, key);

            // Construct the data record.
            GenericData.Record dataRecord = new GenericData.Record(mRecordSchema);
            key.getSchema().getFields().stream().map(Schema.Field::name).forEach(f -> {
                dataRecord.put(f, key.get(f));
            });
            value.getSchema().getFields().stream().map(Schema.Field::name).forEach(f -> {
                dataRecord.put(f, value.get(f));
            });

            if (curNode.height == 0 || curNode.records.size() < mInterval) {
                curNode.addRecord(dataRecord);
                if (curNode.height < mHeight) {
                    curNode = new Node(curNode);
                }
            } else {
                while (curNode.records.size() == mInterval && curNode.height > 0) {
                    flush();
                }
                curNode.addRecord(dataRecord);
                curNode = new Node(curNode);
            }

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            while (curNode != root) {
                flush();
            }
            flush();
            bufferedWriter.reverseAndClose(fileSystem.create(filename));
        }

        private void flush() throws IOException {
            int numRecordsWriting = curNode.records.size();
            logger.debug("writing {} records in height {}, records: {}", numRecordsWriting, curNode.height, curNode.records);
            for (GenericRecord record : curNode.records) {
                bufferedWriter.append(record);
            }
            // the reader will see the blocks backwards, so need to take the sync marker AFTER the block is written:
            long position = bufferedWriter.sync();
            curNode = curNode.prev;
            if (curNode != null) {
                if (numRecordsWriting > 0)
                    curNode.getCurRecord().put(METADATA_COL_NAME, position);
            }
        }
    }

    /**
     * Buffers Avro blocks in memory, and then writes them backwards to real file.
     */
    public static class BufferedWriter {
        private final Schema schema;
        private final ByteArrayOutputStream recordsBuffer;
        private final DataFileWriter<GenericRecord> memoryWriter;
        private final Writer.Options options;
        private final LinkedList<Long> syncs;
        private final String keyValueFields;

        private final long headerPosition;

        public BufferedWriter(Writer.Options options, Schema schema, String keyValueFields) throws IOException {
            // the actual byte buffer to write to:
            ByteArrayOutputStream recordsBuffer = new ByteArrayOutputStream(options.initialCapacityMB << 20);
            // the file writer:
            DataFileWriter<GenericRecord> inMemoryWriter = createMemoryFileWriter(options, schema, recordsBuffer);

            this.schema = schema;
            this.recordsBuffer = recordsBuffer;
            this.memoryWriter = inMemoryWriter.setSyncInterval(1 << 20);
            this.headerPosition = inMemoryWriter.sync();
            this.options = options;
            syncs = new LinkedList<>();
            syncs.add(memoryWriter.sync());

            this.keyValueFields = keyValueFields;
        }

        private DataFileWriter<GenericRecord> createMemoryFileWriter(Writer.Options options, Schema schema, ByteArrayOutputStream recordsBuffer) throws IOException {
            GenericData model = options.model;
            DatumWriter<GenericRecord> datumWriter = model.createDatumWriter(schema);
            DataFileWriter<GenericRecord> inMemoryWriter =
                    new DataFileWriter<>(datumWriter)
                            .setSyncInterval(1 << 20)
                            .setCodec(options.getCodec())
                            .create(schema, recordsBuffer);
            return inMemoryWriter;
        }

        public Long sync() throws IOException {
            long sync = memoryWriter.sync();
            if (syncs.getLast() != sync)
                // saving the syncs just to be able to efficiently read the blocks backwards.
                syncs.add(sync);
            return sync - headerPosition;
        }

        public void append(GenericRecord record) throws IOException {
            memoryWriter.append(record);
        }

        public void reverseAndClose(FSDataOutputStream output) throws IOException {
            sync();
            memoryWriter.close();

            try (DataFileWriter fileWriter = new DataFileWriter<>(options.getDataModel().createDatumWriter(schema))) {

                // create seekable file reader from the in memory file:
                DatumReader datumReader = options.model.createDatumReader(schema);
                byte[] rawAvroFileData = recordsBuffer.toByteArray();
                SeekableByteArrayInput input = new SeekableByteArrayInput(rawAvroFileData);
                DataFileReader inMemoryReader = (DataFileReader) DataFileReader.openReader(input, datumReader);

                // create (real) file writer:
                long dataSize = rawAvroFileData.length - headerPosition;
                fileWriter
                        .setMeta(DATA_SIZE_KEY, dataSize) // put data size in metadata:
                        .setMeta(KEY_VALUE_HEADER_NAME, keyValueFields) // put data size in metadata:
                        .setCodec(options.getCodec())
                        .setSyncInterval(1 << 20)
                        .create(schema, output);

                // read blocks backwards, and append to the real file:
                ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
                Iterator<Long> reversedBlocks = syncs.descendingIterator();
                reversedBlocks.next(); // last sync points to end of file, skip it
                while (reversedBlocks.hasNext()) {
                    Long sync = reversedBlocks.next();
                    inMemoryReader.seek(sync);
                    inMemoryReader.hasNext(); // important! forces the reader to load the next block
                    long count = inMemoryReader.getBlockCount();
                    ByteBuffer block = inMemoryReader.nextBlock();
                    assert (count > 0);
                    fileWriter.appendEncoded(block);
                    // appendEncoded ^ only increments block count by 1, so manually increase it to the real value:
                    while (--count > 0) fileWriter.appendEncoded(emptyBuffer);
                    fileWriter.sync();
                }
            }
        }
    }


    public static Schema createSchema(Schema key, Schema value) {
        List<Schema.Field> schemaFields = new ArrayList<>();
        addFromSchema(schemaFields, key);
        addFromSchema(schemaFields, value);
        schemaFields.add(new Schema.Field(
                METADATA_COL_NAME, metadataSchema, metadataSchema.getDoc(), null, Schema.Field.Order.ASCENDING));

        Schema schema = Schema.createRecord("keyValueSchema", "doc", "na", false);
        schema.setFields(schemaFields);
        return schema;
    }

    private static void addFromSchema(List<Schema.Field> schemaFields, Schema srcSchema) {
        srcSchema.getFields().stream().forEach(field -> {
            Schema.Field f = new Schema.Field(
                    field.name(), field.schema(), field.doc(), field.defaultVal(), field.order());
            schemaFields.add(f);
        });
    }

    private static Schema projectSchema(Schema schema, String[] fields) {
        if(fields.length == 0)
            throw new RuntimeException("attempt to create empty schema");
        HashMap<String, Schema.Field> map = new HashMap<>();
        schema.getFields().forEach(f -> map.put(f.name(), f));
        List<Schema.Field> schemaFields =
                Arrays.stream(fields)
                        .map(map::get)
                        .filter(Objects::nonNull)
                        .map(AvroBtreeFile::cloneField)
                        .collect(Collectors.toList());
        if (schemaFields.size() != fields.length)
            throw new RuntimeException("fields are not subset of the schema");
        return Schema.createRecord(schemaFields);
    }

    private static Schema.Field cloneField(Schema.Field f) {
        return new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
    }
}
