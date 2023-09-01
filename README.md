![LDBC Logo](ldbc-logo.png)
# LDBC SNB Interactive v1 driver

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_interactive_v1_driver.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_interactive_v1_driver)

This driver runs the Social Network Benchmark's Interactive v1 workload, including cross-validation and benchmark execution.
The implementations of the workload (with DBMSs such as Neo4j and PostgreSQL) are available in <https://github.com/ldbc/ldbc_snb_interactive_v1_driver>.

## User Guide

Clone and build with Maven:

```bash
git clone https://github.com/ldbc/ldbc_snb_interactive_v1_driver
cd ldbc_snb_interactive_v1_driver
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

For more information, please refer to the [Documentation](https://github.com/ldbc/ldbc_snb_interactive_v1_driver/tree/main/docs).

## Deploying Maven Artifacts

We use a manual process for deploying Maven artifacts.

1. Clone the [`snb-mvn` repository](https://github.com/ldbc/snb-mvn) next to the driver repository's directory.

2. In the driver repository, run:

    ```bash
    scripts/package-mvn-artifacts.sh
    ```

3. Go to the `snb-mvn` directory, check whether the JAR files are correct.

4. Commit and push.
