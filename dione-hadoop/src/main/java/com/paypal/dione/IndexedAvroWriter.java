package com.paypal.dione;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.paypal.dione.hdfs.index.HdfsIndexContants.*;

/**
 *
 */
public class IndexedAvroWriter extends RecordWriter<GenericRecord, NullWritable> {

    private static final Schema avroConsFields;

    static {
        try {
            avroConsFields = new Schema.Parser().parse(IndexedAvroWriter.class.getClassLoader().getResourceAsStream("indexschema.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load template schema");
        }
    }

    private Set<String> payloadFields;
    private boolean avroPayloadOnly;
    private final ParquetWriter<GenericRecord> parquetWriter;
    private final AvroKeyRecordWriter<GenericRecord> avroWriter;
    private final Schema indexSchema;
    private final Schema avroSchema;
    private final String avroFile;
    private final TaskMetrics taskMetrics;
    private Long lastSync = 0L;

    public IndexedAvroWriter(Schema inputSchema, Path avroFile, Path indexPath, Set<String> payloadFields, boolean avroPayloadOnly,
                             GenericData dataModel, CodecFactory compressionCodec, OutputStream avroOutputStream,
                             Configuration conf, TaskMetrics taskMetrics) throws IOException {


        this.payloadFields = payloadFields; // fields to remove from the index
        this.avroPayloadOnly = avroPayloadOnly; // if true, avro will include ONLY the payload fields
        if (avroPayloadOnly)
            this.avroSchema = generatePayloadSchema(inputSchema);
        else
            this.avroSchema = inputSchema;

        this.indexSchema = generateIndexSchema(inputSchema, conf.get("avro.indexed.index.schemaName", "topLevelRecord"));
        this.avroFile = avroFile.toString();
        this.avroWriter = new AvroKeyRecordWriter<>(this.avroSchema, dataModel, compressionCodec, avroOutputStream);
        this.taskMetrics = taskMetrics;
        this.parquetWriter = AvroParquetWriter.<GenericRecord>builder(indexPath).withSchema(this.indexSchema).withConf(conf).build();
    }

    @Override
    public void write(GenericRecord inputRecord, NullWritable nullWritable) throws IOException {
        AvroKey<GenericRecord> avroRecord;
        if (avroPayloadOnly) {  // remove non-payload fields from the record
            avroRecord = new AvroKey(new GenericData.Record(avroSchema));
            payloadFields.forEach(field -> avroRecord.datum().put(field, inputRecord.get(field)));
        } else avroRecord = new AvroKey(inputRecord);

        long offset = avroWriter.sync();
        long size = offset - lastSync; // good enough
        lastSync = offset;
        validateRowSize(size);

        avroWriter.write(avroRecord, null);
        GenericRecord indexRecord = createIndexRecord(inputRecord, offset, size);
        parquetWriter.write(indexRecord);
        taskMetrics.incBytes(size);
        taskMetrics.incRecords(1L);

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
        avroWriter.close(taskAttemptContext);
        parquetWriter.close();
    }

    private GenericRecord createIndexRecord(GenericRecord payloadRecord, long offset, long payloadBytes) {
        GenericData.Record indexRecord = new GenericData.Record(indexSchema);
        indexSchema.getFields().stream().map(Schema.Field::name).forEach(field ->
                indexRecord.put(field, payloadRecord.get(field)));   // put index columns

        indexRecord.put(FILE_NAME_COLUMN, avroFile);
        indexRecord.put(OFFSET_COLUMN, offset);
        indexRecord.put(SIZE_COLUMN, (int) payloadBytes);

        return indexRecord;
    }

    public Long getParquetDataSize() {  // useful for compaction
        return parquetWriter.getDataSize();
    }

    private Schema generatePayloadSchema(Schema inputSchema) {
        return generateSchema(inputSchema, true, inputSchema.getName(), Arrays.asList());
    }

    private Schema generateIndexSchema(Schema inputSchema, String name) {

        List<Schema.Field> fields = avroConsFields.getFields().stream().map(this::cloneField).collect(Collectors.toList());
        return generateSchema(inputSchema, false, name, fields);
    }

    private Schema.Field cloneField(Schema.Field f) {
        return new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue(), f.order());
    }

    private void validateRowSize(long size) {
        if (size > Integer.MAX_VALUE) { //although payloads larger than tens/hundreds MB also not make sense
            throw new UnsupportedOperationException("Record is too big! Size: " + size + " bytes");
        }
    }

    private Schema generateSchema(Schema inputSchema, boolean isPayload, String name, List<Schema.Field> additionalFields) {
        List<Schema.Field> fields = inputSchema.getFields().stream()
                .filter(field -> isPayload == payloadFields.contains(field.name()))
                .map(this::cloneField)
                .collect(Collectors.toList());

        Schema schema = Schema.createRecord(name, null, name, false);
        schema.setFields(ListUtils.union(fields, additionalFields));
        return schema;
    }

    public interface TaskMetrics {
        void incBytes(Long b);

        void incRecords(Long r);
    }
}
