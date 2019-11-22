![LDBC Logo](ldbc-logo.png)

[![Build Status](https://travis-ci.org/ldbc/ldbc_snb_driver.svg?branch=master)](https://travis-ci.org/ldbc/ldbc_snb_driver)

This driver is being developed as part of the Linked Data Benchmark Council EU-funded research project and will be used to run the benchmark workloads developed and released by LDBC:

* [LDBC Project Website](http://ldbcouncil.org/)
* [LDBC Twitter Account](https://twitter.com/LDBCouncil)
* [LDBC Facebook Page](https://www.facebook.com/ldbcouncil/)

### Branches

:warning: The development of this repository happens on the [`dev` branch](https://github.com/ldbc/ldbc_snb_driver/tree/dev) which contains the current `SNAPSHOT` version. The work-in-progress LDBC implementations typically depend on this version.

### User Guide

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
