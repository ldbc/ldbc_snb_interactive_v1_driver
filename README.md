![LDBC Logo](ldbc-logo.png)
# LDBC SNB Interactive driver

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_interactive_driver.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_interactive_driver)

This driver was developed as part of the Linked Data Benchmark Council EU-funded research project and is be used to run the Social Network Benchmark's Interactive workload.

Related repositories:

* Data generator: https://github.com/ldbc/ldbc_snb_datagen_hadoop
* Implementations: https://github.com/ldbc/ldbc_snb_interactive_impls

Note that the SNB's Business Intelligence (BI) workload uses a different [data generator](https://github.com/ldbc/ldbc_snb_datagen_spark) and [driver](https://github.com/ldbc/ldbc_snb_bi)

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
    ./package-mvn-artifacts.sh
    ```

3. Go to the `snb-mvn` directory, check whether the JAR files are correct.

4. Commit and push.
