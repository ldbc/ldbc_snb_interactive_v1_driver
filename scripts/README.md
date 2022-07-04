# Converter

Scripts to convert from BI to Interactive update streams.
These scripts merge daily batches into a single file, sort by `creationDate` and pre-join attributes/edges (e.g. `tagIds`, `workAt`).

Currently, the inserts are handled using DuckDB and the deletes are handled using pandas.

## Prerequisites

Install DuckDB:

```bash
pip3 install --user duckdb pandas
```

## Script usage

To use the data conversion script `convert_spark_dataset_to_interactive.py`, use the command like the following:

```bash
python3 convert_spark_dataset_to_interactive.py --input_dir '/data/out-sf1' --output_dir '/data/out-sf1/graphs/csv/bi/composite-merged-fk'
```

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

Set `${DATA_ROOT_DIRECTORY}` to the unpacked directory containing the data e.g. '/data/out-sf1' and run the script:

```bash
export DATA_ROOT_DIRECTORY=...
./convert.sh
```
