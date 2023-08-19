![LDBC Logo](ldbc-logo.png)
# LDBC SNB Interactive driver

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_interactive_driver.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_interactive_driver)

This repository has two main branches:
* The [`v2-dev` branch](https://github.com/ldbc/ldbc_snb_interactive_driver/tree/v2-dev) contains the code for SNB Interactive v2, which extends the workload with delete operations and supports larger scale factors. It is still under active development. This workload uses the [LDBC SNB Spark data generator](https://github.com/ldbc/ldbc_snb_datagen_spark).
* The [`v1-dev` branch](https://github.com/ldbc/ldbc_snb_interactive_driver/tree/v1-dev) contains Interactive v1, the stable and auditable version of the workload. This uses the [Hadoop-based generator](https://github.com/ldbc/ldbc_snb_datagen_hadoop).

The implementations of the workload (with DBMSs such as Neo4j and PostgreSQL) are available in <https://github.com/ldbc/ldbc_snb_interactive_impls>.

## Notes

This driver was developed as part of the Linked Data Benchmark Council EU-funded research project and is be used to run the Social Network Benchmark's Interactive workload.

Note that the SNB's Business Intelligence (BI) workload uses a different [data generator](https://github.com/ldbc/ldbc_snb_datagen_spark) and [driver](https://github.com/ldbc/ldbc_snb_bi)

## User Guide

Clone and build with Maven:

```bash
git clone https://github.com/ldbc/ldbc_snb_interactive_driver
cd ldbc_snb_interactive_driver
git checkout v1-dev
mvn clean package
```

To quickly test the driver try the "simpleworkload" that is shipped with it by doing the following:

```bash
java \
  -cp target/driver-standalone.jar org.ldbcouncil.snb.driver.Client \
  -db org.ldbcouncil.snb.driver.workloads.simple.db.SimpleDb \
  -P target/classes/configuration/simple/simpleworkload.properties \
  -P target/classes/configuration/ldbc_driver_default.properties
```

For more information, please refer to the [Documentation](https://github.com/ldbc/ldbc_snb_interactive_driver/tree/v1-dev/docs).

## Deploying Maven Artifacts

We use a manual process for deploying Maven artifacts.

1. Clone the [`snb-mvn` repository](https://github.com/ldbc/snb-mvn) next to the driver repository's directory.

2. In the driver repository, run:

    ```bash
    scripts/package-mvn-artifacts.sh
    ```

3. Go to the `snb-mvn` directory, check whether the JAR files are correct.

4. Commit and push.
