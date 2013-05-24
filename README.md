![LDBC Logo](ldbc_logo.png) 

# LDBC Driver


### Credit where it's due
Though it has now diverged significantly, this project started as a fork of the YCSB framework:
* http://wiki.github.com/brianfrankcooper/YCSB/
* http://research.yahoo.com/Web_Information_Management/YCSB

### LDBC
It is still at an early stage of development, but the LDBC Driver is being developed as part of the Linked Data Benchmark Council EU-funded research project and will be used to run the benchmark workloads developed and released by LDBC:
* http://www.ldbc.eu
* http://www.linkedin.com/groups/LDBC-4955240
* https://twitter.com/LDBCproject
* https://www.facebook.com/ldbcproject

### Try it:

    git clone git@github.com:alexaverbuch/ldbc_driver.git
    cd ldbc_driver
    mvn clean package


At the moment only one workload is supported, "simpleworkload", which has little configuration options and comprises of only basic key/value operations.
To run it:

    java -cp core/target/core-0.1.jar com.ldbc.driver.Client -db com.ldbc.driver.db.basic.BasicDb -s -P workloads/simpleworkload -t
