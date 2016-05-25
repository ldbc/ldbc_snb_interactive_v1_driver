![LDBC Logo](ldbc-logo.png) 

This driver is being developed as part of the Linked Data Benchmark Council EU-funded research project and will be used to run the benchmark workloads developed and released by LDBC:
* [LDBC Project Website](http://ldbcouncil.org/)
* [LDBC Company Website](http://ldbcouncil.org)
* [LDBC LinkedIn Group](http://www.linkedin.com/groups/LDBC-4955240)
* [LDBC Twitter Account](https://twitter.com/LDBCouncil)
* [LDBC Facebook Page](https://www.facebook.com/ldbcouncil/)

### Try it

    git clone https://github.com/ldbc/ldbc_driver.git
    cd ldbc_driver
    mvn clean package -DskipTests

To quickly test the driver try the "simpleworkload" that is shipped with it by doing the following:

	java -cp target/jeeves-0.2-SNAPSHOT.jar com.ldbc.driver.Client -db com.ldbc.driver.workloads.simple.db.BasicDb -P workloads/simple/simpleworkload.properties -P workloads/ldbc_driver_default.properties

For more information, please refer to the [Documentation](https://github.com/ldbc/ldbc_driver/wiki)
