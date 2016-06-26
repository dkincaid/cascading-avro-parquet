package com.idexx.parquet;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import org.apache.avro.Schema;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.avro.AvroDataSupplier;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.cascading.ParquetValueScheme;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Created by davek on 6/14/16.
 */
public class ParquetAvroScheme<T extends AvroDataSupplier> extends ParquetValueScheme<T> {

    private String schema;
    private String compression;
    private Integer blockSize;

    public ParquetAvroScheme() {
        this(new Config<T>());
    }

    public ParquetAvroScheme(Schema schema) {
        super(new Config<T>());
        this.schema = schema.toString();
    }

    public ParquetAvroScheme(Schema schema, CompressionCodecName compression) {
        super(new Config<T>());
        this.schema = schema.toString();
        this.compression = compression.name();
    }

    public ParquetAvroScheme(Schema schema, CompressionCodecName compression, int blockSize) {
        super(new Config<T>());
        this.schema = schema.toString();
        this.compression = compression.name();
        this.blockSize = blockSize;
    }

    public ParquetAvroScheme(Config<T> config) {
        super(config);
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> fp,
                               Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
        super.sourceConfInit(fp, tap, jobConf);
        jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(jobConf, AvroReadSupport.class);
        jobConf.set("parquet.avro.schema", schema);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> fp,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
        if (schema == null) {
            throw new IllegalArgumentException("To use ParquetAvroScheme as a sink, you must specify a schema in the constructor");
        }

        DeprecatedParquetOutputFormat.setAsOutputFormat(jobConf);
        DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, AvroWriteSupport.class);
        DeprecatedParquetOutputFormat.setCompression(jobConf, CompressionCodecName.valueOf(compression));
        if (blockSize != null) {
            DeprecatedParquetOutputFormat.setBlockSize(jobConf, blockSize);
        }
        jobConf.set("parquet.avro.schema", schema);
    }
}
