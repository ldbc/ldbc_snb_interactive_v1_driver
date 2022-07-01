# Converter

Scripts to convert from BI to Interactive update streams.
These scripts merge daily batches into a single file, sort by `creationDate` and pre-join attributes/edges (e.g. `tagIds`, `workAt`).

Currently, the inserts are handled using DuckDB and the deletes are handled using pandas.

## Data generation

Set the desired scale factor and cleanup, e.g.:

```bash
export SF=0.1
rm -rf out-sf${SF}/graphs/parquet/raw
```

Generate the data set:

```bash
tools/run.py \
    --cores 4 \
    --memory 8G \
    ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
    -- \
    --format csv \
    --scale-factor ${SF} \
    --mode bi \
    --output-dir out-sf${SF}/
```

Set `${DATA_ROOT_DIRECTORY}` to the unpacked directory containing the data e.g. '/data/out-sf1':

```bash
export DATA_ROOT_DIRECTORY=...
```

## Deletes

```bash
python convert_spark_deletes_to_interactive.py ${DATA_ROOT_DIRECTORY} 
```

## Inserts

### Prerequisites

Install DuckDB:

```bash
pip3 install --user duckdb==0.3.4
```

### Running the conversion

```bash
mkdir inserts
python convert.py --input_dir ${DATA_ROOT_DIRECTORY} --output_dir inserts
```
