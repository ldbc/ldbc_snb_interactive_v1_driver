# Parameter generation

The paramgen implements [parameter curation](https://research.vu.nl/en/publications/parameter-curation-for-benchmark-queries) to ensure predictable performance results where the runtimes of a given query variant (roughly) correspond to a normal distribution.

## Getting started

1. Install dependencies:

    ```bash
    scripts/install-dependencies.sh
    ```

1. **Generating the factors with the Datagen:** In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands. We assume that the Datagen project is built and `sbt` is available.

    ```bash
    export SF=desired_scale_factor
    export LDBC_SNB_DATAGEN_MAX_MEM=available_memory
    export LDBC_SNB_DATAGEN_JAR=$(sbt -batch -error 'print assembly / assemblyOutputPath')
    ```

    ```bash
    rm -rf out-sf${SF}/{factors,graphs/parquet/raw}
    tools/run.py \
        --cores $(nproc) \
        --memory ${LDBC_SNB_DATAGEN_MAX_MEM} \
        -- \
        --format parquet \
        --scale-factor ${SF} \
        --mode raw \
        --output-dir out-sf${SF} \
        --generate-factors
    ```

1. **Obtaining the factors:** Cleanup the `factors/` directory and move the factor directories from `out-sf${SF}/factors/parquet/raw/composite-merged-fk/` (`personFirstNames`, `personNumFriendOfFriendPosts/`, etc.) to the `factors/` directory. Assuming that your `${LDBC_SNB_DATAGEN_DIR}` and `${SF}` environment variable set, run:

    ```bash
    export LDBC_SNB_DATA_ROOT_DIRECTORY=${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/
    export LDBC_SNB_FACTOR_TABLES_DIR=${LDBC_SNB_DATA_ROOT_DIRECTORY}/factors/parquet/raw/composite-merged-fk/
    ```

    To download and use the sample (SF0.003) raw data set and its factors, run:

    ```bash
    paramgen/get-sample-data.sh
    . scripts/use-sample-data.sh
    ```

2. To run the parameter generator, issue:

    ```bash
    paramgen/paramgen.sh
    ```

3. The parameters will be placed in the `parameters/` directory.

## Previewing the Parquet files with DuckDB

Ensure that the `duckdb` CLI binary is on your path. Then, you can run e.g.:

```bash
echo "SELECT * FROM '../parameters/interactive-13a.parquet' LIMIT 1;" | duckdb
```

To preview all results, use:

```bash
scripts/preview.sh
```
