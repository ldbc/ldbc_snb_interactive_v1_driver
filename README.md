![LDBC Logo](ldbc-logo.png)
# LDBC SNB Interactive driver

[![Build Status](https://circleci.com/gh/ldbc/ldbc_snb_driver.svg?style=svg)](https://circleci.com/gh/ldbc/ldbc_snb_driver)

This driver was developed as part of the Linked Data Benchmark Council EU-funded research project and is be used to run the Social Network Benchmark's Interactive workload.

Related repositories:

* Data generator: https://github.com/ldbc/ldbc_snb_datagen_hadoop
* Implementations: https://github.com/ldbc/ldbc_snb_interactive

Note that the SNB's Business Intelligence (BI) workload uses a different (Python-based) driver: https://github.com/ldbc/ldbc_snb_bi

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
  -cp target/jeeves-standalone.jar com.ldbc.driver.Client \
  -db com.ldbc.driver.workloads.simple.db.SimpleDb \
  -P target/classes/configuration/simple/simpleworkload.properties \
  -P target/classes/configuration/ldbc_driver_default.properties
```

For more information, please refer to the [Documentation](https://github.com/ldbc/ldbc_driver/wiki).
