# Converter

Scripts to convert from BI to Interactive update streams.
These scripts merge daily batches into a single file, sort by `creationDate` and pre-join attributes/edges (e.g. `tagIds`, `workAt`).

Currently, the inserts are handled using DuckDB and the deletes are handled using pandas.

## Deletes

Run:

```bash
python convert_spark_deletes_to_interactive.py
```

## Inserts

### Prerequisites

Install DuckDB:

```bash
pip3 install --user duckdb==0.3.4
```

### Usage

Generate the data set:

```bash
export SF=0.1
```

```bash
tools/run.py \
    --cores 4 \
    --memory 8G \
    ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
    -- \
    --format csv \
    --scale-factor ${SF} \
    --explode-edges \
    --mode bi \
    --output-dir out-sf${SF}/
```

Run the converter in this directory:

```bash
mkdir out-sf${SF}-test
python convert.py --input_dir ../out-sf${SF} --output_dir out-sf${SF}-test
```
