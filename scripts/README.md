# Converter

The LDBC SNB Interactive 2.0 workloads uses the [SNB Spark Datagen](https://github.com/ldbc/ldbc_snb_datagen_spark) for generating data sets. Namely, it uses the `bi` mode of the Datagen, then uses DuckDB SQL queries (through a Python script) to convert their insert/delete streams into the formats required by the Interactive 2.0 driver.

The script in this directory uses the raw parquet files produced by the `bi` mode, sort the rows by `creationDate` and pre-join edges with a many-to-many cardinality (e.g. insertin a `Person` node has a list of `tagIds` and `workAt` edges).

## Prerequisites

Install DuckDB:

```bash
scripts/install-dependencies.sh
```
## Data generation

Set the desired scale factor and cleanup, e.g.:

```bash
export SF=0.1
rm -rf out-sf${SF}/graphs/parquet/raw
```

Generate the data set:

```bash
export LDBC_SNB_DATAGEN_MAX_MEM=...
export LDBC_SNB_DATAGEN_JAR=$(sbt -batch -error 'print assembly / assemblyOutputPath')
tools/run.py \
    --parallelism $(nproc) \
    --memory ${LDBC_SNB_DATAGEN_MAX_MEM} \
    -- \
    --format parquet \
    --scale-factor ${SF} \
    --mode bi \
    --output-dir out-sf${SF}/
```

## Data conversion

Set `${LDBC_SNB_DATA_ROOT_DIRECTORY}` to the unpacked directory containing the raw data e.g. '/data/out-sf1/graphs/parquet/raw' and run the script:

```bash
export LDBC_SNB_DATA_ROOT_DIRECTORY=...
./convert.sh
```
