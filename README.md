# Parquet Cascading tap using Avro records
This small project contains only a single class, `com.idexx.parquet.ParquetAvroScheme`, that can be used as a source or sink for reading or
writing Parquet files using Avro records in a Cascading flow.

Note that currently it only works with GenericRecord's and I've only tested it as a sink from a
Cascalog query. The output of your Cascading flow (or Cascalog query) should be a single field that
is an Avro GenericRecord.

I used the ParquetTBaseScheme class from the parquet-mr/parquet-cascading module as a guide.

#License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

