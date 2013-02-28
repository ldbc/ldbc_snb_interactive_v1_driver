## Neo4j YCSB Client - Quick Start

This section describes how to run YCSB on Neo4j running on a server.

### 1. Start Neo4j Server

For details:
http://docs.neo4j.org/chunked/milestone/server.html

By default Neo4jClient will use the following URL:
http://localhost:7474/db/data

### 2. Set Up YCSB

Clone the YCSB git repository and compile:

	git clone git://github.com/alexaverbuch/YCSB.git
    	cd YCSB
    	mvn clean package

### 3. Build YCSB

Clone the YCSB git repository and compile:

	git clone git://github.com/brianfrankcooper/YCSB.git
    	cd YCSB
	mvn clean package

### 4. Run YCSB

Now you are ready to run! 

First, load the data:
	./bin/ycsb load neo4j -P workloads/workloada -s -p operationcount=100 -p recordcount=100 -p neo4j.clear=true

Then, run the workload:
	./bin/ycsb run neo4j -P workloads/workloada -s -p operationcount=100 -p recordcount=100

## 5. Neo4j Configuration Parameters

#### `neo4j.url` (default: `http://localhost:7474/db/data`) --> location of neo4j server

#### `neo4j.primarykey` (default: `primarykey`) --> property to index

#### `neo4j.table` (default: `usertable`) --> name of table (i.e. label)

#### `neo4j.clear` (default: `false`) --> clear database
