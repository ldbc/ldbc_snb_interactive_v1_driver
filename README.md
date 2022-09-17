![LDBC Logo](ldbc-logo.png)
# LDBC SNB Interactive driver

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_interactive_driver.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_interactive_driver)

This driver runs the Social Network Benchmark's Interactive workload, including cross-validation and benchmark execution.

The `main` branch of this repository contains the code for SNB Interactive v2.0, which extends the workload with delete operations and supports larger scale factors. It is still under active development.
If you are looking for a stable and auditable version, please check out the [`v1-dev` branch](https://github.com/ldbc/ldbc_snb_interactive_driver/tree/v1-dev).

Related repositories:

* Data generator: https://github.com/ldbc/ldbc_snb_datagen_spark
* Implementations: https://github.com/ldbc/ldbc_snb_interactive_impls

Note that v2.0 of SNB Interactive uses the [LDBC SNB Spark data generator](https://github.com/ldbc/ldbc_snb_datagen_spark) (while older versions used the [Hadoop-based generator](https://github.com/ldbc/ldbc_snb_datagen_hadoop)).

### User Guide

Clone and build with Maven:

```bash
git clone https://github.com/ldbc/ldbc_snb_driver.git
cd ldbc_snb_driver
mvn clean package -DskipTests
```

To quickly test the driver try the "simpleworkload" that is shipped with it by doing the following:

```bash
java \
  -cp target/driver-standalone.jar org.ldbcouncil.snb.driver.Client \
  -db org.ldbcouncil.snb.driver.workloads.simple.db.SimpleDb \
  -P target/classes/configuration/simple/simpleworkload.properties \
  -P target/classes/configuration/ldbc_driver_default.properties
```

For more information, please refer to the [Documentation](https://github.com/ldbc/ldbc_driver/wiki).

### Deploying Maven Artifacts

We use a manual process for deploying Maven artifacts.

1. Clone the [`snb-mvn` repository](https://github.com/ldbc/snb-mvn) next to the driver repository's directory.

2. In the driver repository, run:

    ```bash
    scripts/package-mvn-artifacts.sh
    ```

3. Go to the `snb-mvn` directory, check whether the JAR files are correct.

4. Commit and push.
