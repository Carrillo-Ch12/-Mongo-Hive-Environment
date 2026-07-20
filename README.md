# Preparing a MongoDB + Hive Experimental Environment

Complete guide to install, configure and start up the experimental environment
used in the energy-efficiency study on MongoDB 8.0 and Apache Hive 3.1.3 with the
TPC-H benchmark (SF=5).

---

## Table of contents

- [Hardware requirements](#hardware-requirements)
- [Software requirements](#software-requirements)
- [1. Initial node configuration](#1-initial-node-configuration)
- [2. MongoDB — Sharded Cluster](#2-mongodb--sharded-cluster)
- [3. MongoDB — Centralized Mode](#3-mongodb--centralized-mode)
- [4. Hadoop + Hive — Fragmented Cluster](#4-hadoop--hive--fragmented-cluster)
- [5. Hadoop + Hive — Centralized Mode](#5-hadoop--hive--centralized-mode)
- [6. Scaphandre](#6-scaphandre)
- [7. TPC-H data generation](#7-tpc-h-data-generation)
- [8. Python dependencies](#8-python-dependencies)
- [9. Full environment verification](#9-full-environment-verification)

---

## Hardware requirements

| Component        | Minimum                                              | Recommended                              |
| ---------------- | ---------------------------------------------------- | ---------------------------------------- |
| Nodes            | 3                                                    | 3 or more                                |
| Processor        | Intel Core i3 6th gen (2 cores, 4 threads) ≥3.0 GHz  | Intel Core i5/i7 8th gen (4+ cores)      |
| RAM per node     | 12 GB DDR4                                           | 32 GB DDR4                               |
| Storage          | SSD ≥240 GB                                          | NVMe SSD ≥500 GB                         |
| Operating System | Ubuntu 20.04 LTS (64-bit)                            | Ubuntu 22.04 / 24.04 LTS                 |
| Network          | 100 Mbps Ethernet, static IP per node                | Gigabit Ethernet with a dedicated switch |
| Connectivity     | Passwordless SSH between all nodes                    | Passwordless SSH between all nodes       |

> **Mandatory requirement:** the processor must support **Intel RAPL** (*Running Average Power Limit*), required by Scaphandre for energy-consumption measurement. Intel Skylake (6th gen) processors or later meet this requirement. For AMD, the Linux kernel `amd_energy` module is required.

---

## Software requirements

| Component      | Minimum version | Version used |
| -------------- | --------------- | ------------ |
| Java (OpenJDK) | 8               | 11           |
| Python         | 3.8             | 3.10         |
| MongoDB        | 7.0             | 8.0.17       |
| Apache Hadoop  | 3.3.0           | 3.3.6        |
| Apache Hive    | 3.1.0           | 3.1.3        |
| Scaphandre     | 2.0.0           | 2.6.0        |
| TPC-H dbgen    | 2.18.0          | 2.18.0       |
| Git            | 2.x             | —            |
| Bash           | 5.x             | —            |

**Python libraries:**

| Library    | Minimum version | Version used |
| ---------- | --------------- | ------------ |
| pymongo    | 4.0             | 4.7.0        |
| pyhive     | 0.6.5           | 0.6.5        |
| pandas     | 1.3.0           | 2.2.2        |
| numpy      | 1.21.0          | 2.0.2        |
| scipy      | 1.7.0           | 1.13.0       |
| matplotlib | 3.4.0           | 3.9.0        |
| seaborn    | 0.11.0          | 0.13.2       |

---

## 1. Initial node configuration

Run on the **three nodes**:

```
# Update the system
sudo apt update && sudo apt upgrade -y

# Install base dependencies
sudo apt install -y openjdk-11-jdk python3 python3-pip git \
    build-essential wget curl

# Verify Java
java -version

# Configure /etc/hosts on the three nodes
echo "10.150.45.251  nodo1" | sudo tee -a /etc/hosts
echo "10.150.45.252  nodo2" | sudo tee -a /etc/hosts
echo "10.150.45.253  nodo3" | sudo tee -a /etc/hosts
```

Set up passwordless SSH from each node to all the others:

```
# On each node, generate an SSH key
ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa

# Copy the public key to all nodes
ssh-copy-id usuario@nodo1
ssh-copy-id usuario@nodo2
ssh-copy-id usuario@nodo3

# Verify connectivity
ssh nodo1 "hostname"
ssh nodo2 "hostname"
ssh nodo3 "hostname"
```

---

## 2. MongoDB — Sharded Cluster

Install MongoDB 8.0 on the **three nodes**:

```
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc \
  | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" \
  | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

sudo apt update && sudo apt install -y mongodb-org
```

### Config Server (nodo1)

Create `/etc/mongod_configsvr.conf`:

```
storage:
  dbPath: /var/lib/mongodb/configsvr
net:
  port: 27019
  bindIp: 0.0.0.0
replication:
  replSetName: configReplSet
sharding:
  clusterRole: configsvr
```

```
sudo mongod --config /etc/mongod_configsvr.conf --fork \
    --logpath /var/log/mongodb/configsvr.log

# Initialize the config server replica set
mongosh --port 27019 --eval \
  "rs.initiate({_id:'configReplSet', \
  configsvr:true, members:[{_id:0,host:'nodo1:27019'}]})"
```

### Shards (nodo1, nodo2, nodo3)

Create `/etc/mongod_shardN.conf` on each node (replace `N` with 1, 2 or 3):

```
storage:
  dbPath: /var/lib/mongodb/shardN
net:
  port: 27018
  bindIp: 0.0.0.0
replication:
  replSetName: shardNReplSet
sharding:
  clusterRole: shardsvr
```

```
sudo mongod --config /etc/mongod_shardN.conf --fork \
    --logpath /var/log/mongodb/shardN.log

# Initialize each shard's replica set (run on each node)
mongosh --port 27018 --eval \
  "rs.initiate({_id:'shardNReplSet', \
  members:[{_id:0,host:'nodoN:27018'}]})"
```

### Mongos — router (nodo1)

```
mongos --configdb configReplSet/nodo1:27019 \
  --port 27017 --bind_ip 0.0.0.0 \
  --fork --logpath /var/log/mongodb/mongos.log

# Add the shards to the cluster
mongosh --port 27017 --eval "sh.addShard('shard1ReplSet/nodo1:27018')"
mongosh --port 27017 --eval "sh.addShard('shard2ReplSet/nodo2:27018')"
mongosh --port 27017 --eval "sh.addShard('shard3ReplSet/nodo3:27018')"

# Verify cluster status
mongosh --port 27017 --eval "sh.status()"
```

### Enable per-collection sharding (hashed key)

```
// Run in mongosh --port 27017
// Replace "tpch_indices" with the corresponding database name
use tpch_indices

sh.enableSharding("tpch_indices")

sh.shardCollection("tpch_indices.lineitem",  { _id: "hashed" })
sh.shardCollection("tpch_indices.orders",    { _id: "hashed" })
sh.shardCollection("tpch_indices.customer",  { _id: "hashed" })
sh.shardCollection("tpch_indices.part",      { _id: "hashed" })
sh.shardCollection("tpch_indices.supplier",  { _id: "hashed" })
sh.shardCollection("tpch_indices.partsupp",  { _id: "hashed" })
sh.shardCollection("tpch_indices.nation",    { _id: "hashed" })
sh.shardCollection("tpch_indices.region",    { _id: "hashed" })
```

---

## 3. MongoDB — Centralized Mode

For the centralized deployment, MongoDB is configured as a **standalone** instance on `nodo1`, without sharding or replica sets.

Create `/etc/mongod_standalone.conf`:

```
storage:
  dbPath: /var/lib/mongodb/standalone
net:
  port: 27017
  bindIp: 0.0.0.0
```

```
sudo mongod --config /etc/mongod_standalone.conf --fork \
    --logpath /var/log/mongodb/standalone.log

# Verify
mongosh --port 27017 --eval "db.adminCommand({ping:1})"
```

---

## 4. Hadoop + Hive — Fragmented Cluster

### Hadoop 3.3.6 installation (three nodes)

```
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz -C /opt/
export HADOOP_HOME=/opt/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Configure JAVA_HOME in hadoop-env.sh
echo "export JAVA_HOME=$(dirname $(dirname \
  $(readlink -f $(which java))))" \
  >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

### Key configuration files (nodo3 — master)

**`core-site.xml`**

```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://nodo3:9000</value>
  </property>
</configuration>
```

**`hdfs-site.xml`**

```
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/opt/hadoop-3.3.6/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/hadoop-3.3.6/data/datanode</value>
  </property>
</configuration>
```

**`yarn-site.xml`**

```
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>nodo3</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
```

**`workers`** (in `$HADOOP_HOME/etc/hadoop/workers`): nodo2 and nodo3.

### Start HDFS and YARN

```
# Format the NameNode (only the first time)
hdfs namenode -format

# Start the services from nodo3
start-dfs.sh
start-yarn.sh

# Verify (nodo3 must show NameNode and ResourceManager)
jps
# nodo1 and nodo2 must show DataNode and NodeManager
ssh nodo1 "jps"
ssh nodo2 "jps"
```

### Hive 3.1.3 installation (nodo3)

```
wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz -C /opt/
export HIVE_HOME=/opt/apache-hive-3.1.3-bin
export PATH=$PATH:$HIVE_HOME/bin

# Initialize the metastore
schematool -dbType derby -initSchema

# Start HiveServer2
hive --service hiveserver2 &

# Verify the connection
beeline -u jdbc:hive2://nodo3:10000 -e "SHOW DATABASES;"
```

### Create TPC-H tables in ORC format (four variants)

Below is the example for `lineitem`. Repeat for the 8 TPC-H tables (`orders`, `customer`, `part`, `supplier`, `partsupp`, `nation`, `region`).

**Baseline** — ORC without bucketing or compression:

```
CREATE TABLE lineitem_baseline (
  l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT,
  l_linenumber INT, l_quantity DECIMAL(15,2),
  l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2),
  l_tax DECIMAL(15,2), l_returnflag STRING, l_linestatus STRING,
  l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
  l_shipinstruct STRING, l_shipmode STRING, l_comment STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="NONE");
```

**Indexes** — ORC + Bucketing (no compression):

```
CREATE TABLE lineitem_indices ( /* same columns */ )
CLUSTERED BY (l_orderkey) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ("orc.compress"="NONE");
```

**Compression** — ORC + Snappy (no bucketing):

```
CREATE TABLE lineitem_compresion ( /* same columns */ )
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
```

**Indexes+Compression** — ORC + Bucketing + Snappy:

```
CREATE TABLE lineitem_indice_compresion ( /* same columns */ )
CLUSTERED BY (l_orderkey) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
```

Load the data from HDFS into each table:

```
LOAD DATA INPATH '/tpch/lineitem.tbl' INTO TABLE lineitem_baseline;
-- Repeat for each variant and each table
```

---

## 5. Hadoop + Hive — Centralized Mode

For the centralized deployment, Hadoop is configured in **single-node pseudo-distributed** mode on `nodo3`.

Modify `hdfs-site.xml`:

```
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
```

Modify `workers` so that it contains only `nodo3`.
The rest of the installation procedure and table creation is identical to the fragmented cluster.

---

## 6. Scaphandre

Install on the **three nodes**:

```
# Verify Intel RAPL support
ls /sys/class/powercap/intel-rapl/

# Download and install Scaphandre 2.6.0
wget https://github.com/hubblo-org/scaphandre/releases/\
download/v2.6.0/scaphandre-2.6.0-x86_64-linux.tar.gz
tar -xzf scaphandre-2.6.0-x86_64-linux.tar.gz
sudo mv scaphandre /usr/local/bin/

# Verify the installation
scaphandre --version

# Measurement test (10 samples, 2 s interval)
sudo scaphandre json --step 2 --max-top-consumers 5 \
    --timeout 20 > /tmp/test_scaphandre.json
```

---

## 7. TPC-H data generation

```
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make

# Generate SF=5 (~5 GB)
./dbgen -s 5

# Verify the generated files (expected: 8 .tbl files, ~5 GB total)
ls -lh *.tbl
```

Load the `.tbl` files into HDFS for Hive:

```
hdfs dfs -mkdir -p /tpch
hdfs dfs -put *.tbl /tpch/
hdfs dfs -ls /tpch/
```

---

## 8. Python dependencies

```
pip3 install \
  pymongo==4.7.0 \
  pyhive==0.6.5 \
  pandas==2.2.2 \
  numpy==2.0.2 \
  scipy==1.13.0 \
  matplotlib==3.9.0 \
  seaborn==0.13.2
```

---

## 9. Full environment verification

```
# MongoDB sharded cluster
mongosh --port 27017 --eval "sh.status()"

# MongoDB standalone (centralized)
mongosh --port 27017 --eval "db.adminCommand({ping:1})"

# Hadoop HDFS
hdfs dfsadmin -report

# HiveServer2
beeline -u jdbc:hive2://nodo3:10000 -e "SHOW DATABASES;"

# Scaphandre
sudo scaphandre --version

# Python libraries
python3 -c "import pymongo, pyhive, pandas, numpy, scipy; print('OK')"
```
