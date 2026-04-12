# Preparing a MongoDB + Hive Experimental Environment

Guía completa de instalación, configuración y puesta en marcha del entorno
experimental utilizado en el estudio de eficiencia energética sobre MongoDB 8.0
y Apache Hive 3.1.3 con benchmark TPC-H (SF=5).

---

## Tabla de contenidos

- [Requerimientos de hardware](#requerimientos-de-hardware)
- [Requerimientos de software](#requerimientos-de-software)
- [1. Configuración inicial de los nodos](#1-configuración-inicial-de-los-nodos)
- [2. MongoDB — Clúster Sharded](#2-mongodb--clúster-sharded)
- [3. MongoDB — Modo Centralizado](#3-mongodb--modo-centralizado)
- [4. Hadoop + Hive — Clúster Fragmentado](#4-hadoop--hive--clúster-fragmentado)
- [5. Hadoop + Hive — Modo Centralizado](#5-hadoop--hive--modo-centralizado)
- [6. Scaphandre](#6-scaphandre)
- [7. Generación de datos TPC-H](#7-generación-de-datos-tpc-h)
- [8. Dependencias Python](#8-dependencias-python)
- [9. Verificación del entorno completo](#9-verificación-del-entorno-completo)

---

## Requerimientos de hardware

| Componente        | Mínimo                                               | Recomendado                               |
|-------------------|------------------------------------------------------|-------------------------------------------|
| Nodos             | 3                                                    | 3 o más                                   |
| Procesador        | Intel Core i3 6.ª gen (2 núcleos, 4 hilos) ≥3,0 GHz | Intel Core i5/i7 8.ª gen (4+ núcleos)    |
| RAM por nodo      | 12 GB DDR4                                           | 32 GB DDR4                                |
| Almacenamiento    | SSD ≥240 GB                                          | SSD NVMe ≥500 GB                          |
| Sistema Operativo | Ubuntu 20.04 LTS (64 bits)                           | Ubuntu 22.04 / 24.04 LTS                  |
| Red               | Ethernet 100 Mbps, IP fija por nodo                  | Ethernet Gigabit con switch dedicado      |
| Conectividad      | SSH sin contraseña entre todos los nodos             | SSH sin contraseña entre todos los nodos  |

> **Requisito indispensable:** el procesador debe soportar **Intel RAPL**
> (*Running Average Power Limit*), requerido por Scaphandre para la medición
> de consumo energético. Procesadores Intel Skylake (6.ª gen) o posterior
> cumplen este requisito. Para AMD se requiere el módulo `amd_energy` del
> kernel Linux.

---

## Requerimientos de software

| Componente     | Versión mínima | Versión utilizada |
|----------------|----------------|-------------------|
| Java (OpenJDK) | 8              | 11                |
| Python         | 3.8            | 3.10              |
| MongoDB        | 7.0            | 8.0.17            |
| Apache Hadoop  | 3.3.0          | 3.3.6             |
| Apache Hive    | 3.1.0          | 3.1.3             |
| Scaphandre     | 2.0.0          | 2.6.0             |
| TPC-H dbgen    | 2.18.0         | 2.18.0            |
| Git            | 2.x            | —                 |
| Bash           | 5.x            | —                 |

**Bibliotecas Python:**

| Biblioteca | Versión mínima | Versión utilizada |
|------------|----------------|-------------------|
| pymongo    | 4.0            | 4.7.0             |
| pyhive     | 0.6.5          | 0.6.5             |
| pandas     | 1.3.0          | 2.2.2             |
| numpy      | 1.21.0         | 2.0.2             |
| scipy      | 1.7.0          | 1.13.0            |
| matplotlib | 3.4.0          | 3.9.0             |
| seaborn    | 0.11.0         | 0.13.2            |

---

## 1. Configuración inicial de los nodos

Ejecutar en los **tres nodos**:

```bash
# Actualizar el sistema
sudo apt update && sudo apt upgrade -y

# Instalar dependencias base
sudo apt install -y openjdk-11-jdk python3 python3-pip git \
    build-essential wget curl

# Verificar Java
java -version

# Configurar /etc/hosts en los tres nodos
echo "10.150.45.251  nodo1" | sudo tee -a /etc/hosts
echo "10.150.45.252  nodo2" | sudo tee -a /etc/hosts
echo "10.150.45.253  nodo3" | sudo tee -a /etc/hosts
```

Configurar SSH sin contraseña desde cada nodo hacia todos los demás:

```bash
# En cada nodo, generar clave SSH
ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa

# Copiar clave pública a todos los nodos
ssh-copy-id usuario@nodo1
ssh-copy-id usuario@nodo2
ssh-copy-id usuario@nodo3

# Verificar conectividad
ssh nodo1 "hostname"
ssh nodo2 "hostname"
ssh nodo3 "hostname"
```

---

## 2. MongoDB — Clúster Sharded

Instalar MongoDB 8.0 en los **tres nodos**:

```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc \
  | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" \
  | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

sudo apt update && sudo apt install -y mongodb-org
```

### Config Server (nodo1)

Crear `/etc/mongod_configsvr.conf`:

```yaml
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

```bash
sudo mongod --config /etc/mongod_configsvr.conf --fork \
    --logpath /var/log/mongodb/configsvr.log

# Inicializar el replica set del config server
mongosh --port 27019 --eval \
  "rs.initiate({_id:'configReplSet', \
  configsvr:true, members:[{_id:0,host:'nodo1:27019'}]})"
```

### Shards (nodo1, nodo2, nodo3)

Crear `/etc/mongod_shardN.conf` en cada nodo (reemplazar `N` por 1, 2 o 3):

```yaml
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

```bash
sudo mongod --config /etc/mongod_shardN.conf --fork \
    --logpath /var/log/mongodb/shardN.log

# Inicializar replica set de cada shard (ejecutar en cada nodo)
mongosh --port 27018 --eval \
  "rs.initiate({_id:'shardNReplSet', \
  members:[{_id:0,host:'nodoN:27018'}]})"
```

### Mongos — enrutador (nodo1)

```bash
mongos --configdb configReplSet/nodo1:27019 \
  --port 27017 --bind_ip 0.0.0.0 \
  --fork --logpath /var/log/mongodb/mongos.log

# Añadir los shards al clúster
mongosh --port 27017 --eval "sh.addShard('shard1ReplSet/nodo1:27018')"
mongosh --port 27017 --eval "sh.addShard('shard2ReplSet/nodo2:27018')"
mongosh --port 27017 --eval "sh.addShard('shard3ReplSet/nodo3:27018')"

# Verificar estado del clúster
mongosh --port 27017 --eval "sh.status()"
```

### Habilitar sharding por colección (clave hash)

```javascript
// Ejecutar en mongosh --port 27017
// Reemplazar "tpch_indices" por el nombre de BD correspondiente
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

## 3. MongoDB — Modo Centralizado

Para el despliegue centralizado, MongoDB se configura como instancia
**standalone** en `nodo1`, sin sharding ni replica sets.

Crear `/etc/mongod_standalone.conf`:

```yaml
storage:
  dbPath: /var/lib/mongodb/standalone
net:
  port: 27017
  bindIp: 0.0.0.0
```

```bash
sudo mongod --config /etc/mongod_standalone.conf --fork \
    --logpath /var/log/mongodb/standalone.log

# Verificar
mongosh --port 27017 --eval "db.adminCommand({ping:1})"
```

---

## 4. Hadoop + Hive — Clúster Fragmentado

### Instalación de Hadoop 3.3.6 (tres nodos)

```bash
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz -C /opt/
export HADOOP_HOME=/opt/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Configurar JAVA_HOME en hadoop-env.sh
echo "export JAVA_HOME=$(dirname $(dirname \
  $(readlink -f $(which java))))" \
  >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

### Archivos de configuración clave (nodo3 — master)

**`core-site.xml`**
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://nodo3:9000</value>
  </property>
</configuration>
```

**`hdfs-site.xml`**
```xml
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
```xml
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

**`workers`** (en `$HADOOP_HOME/etc/hadoop/workers`):

# Nodo 2 y 3 

### Iniciar HDFS y YARN

```bash
# Formatear NameNode (solo la primera vez)
hdfs namenode -format

# Iniciar servicios desde nodo3
start-dfs.sh
start-yarn.sh

# Verificar (nodo3 debe mostrar NameNode y ResourceManager)
jps
# nodo1 y nodo2 deben mostrar DataNode y NodeManager
ssh nodo1 "jps"
ssh nodo2 "jps"
```

### Instalación de Hive 3.1.3 (nodo3)

```bash
wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz -C /opt/
export HIVE_HOME=/opt/apache-hive-3.1.3-bin
export PATH=$PATH:$HIVE_HOME/bin

# Inicializar metastore
schematool -dbType derby -initSchema

# Levantar HiveServer2
hive --service hiveserver2 &

# Verificar conexión
beeline -u jdbc:hive2://nodo3:10000 -e "SHOW DATABASES;"
```

### Crear tablas TPC-H en formato ORC (cuatro variantes)

A continuación se muestra el ejemplo para `lineitem`. Repetir para las
8 tablas TPC-H (`orders`, `customer`, `part`, `supplier`, `partsupp`,
`nation`, `region`).

**Baseline** — ORC sin bucketing ni compresión:
```sql
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

**Índices** — ORC + Bucketing (sin compresión):
```sql
CREATE TABLE lineitem_indices ( /* mismas columnas */ )
CLUSTERED BY (l_orderkey) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ("orc.compress"="NONE");
```

**Compresión** — ORC + Snappy (sin bucketing):
```sql
CREATE TABLE lineitem_compresion ( /* mismas columnas */ )
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
```

**Índices+Compresión** — ORC + Bucketing + Snappy:
```sql
CREATE TABLE lineitem_indice_compresion ( /* mismas columnas */ )
CLUSTERED BY (l_orderkey) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
```

Cargar datos desde HDFS en cada tabla:
```sql
LOAD DATA INPATH '/tpch/lineitem.tbl' INTO TABLE lineitem_baseline;
-- Repetir para cada variante y cada tabla
```

---

## 5. Hadoop + Hive — Modo Centralizado

Para el despliegue centralizado, Hadoop se configura en modo
**pseudo-distribuido de nodo único** en `nodo3`.

Modificar `hdfs-site.xml`:
```xml
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
```

Modificar `workers` para que contenga únicamente `nodo3`.
El resto del procedimiento de instalación y creación de tablas
es idéntico al clúster fragmentado.

---

## 6. Scaphandre

Instalar en los **tres nodos**:

```bash
# Verificar soporte Intel RAPL
ls /sys/class/powercap/intel-rapl/

# Descargar e instalar Scaphandre 2.6.0
wget https://github.com/hubblo-org/scaphandre/releases/\
download/v2.6.0/scaphandre-2.6.0-x86_64-linux.tar.gz
tar -xzf scaphandre-2.6.0-x86_64-linux.tar.gz
sudo mv scaphandre /usr/local/bin/

# Verificar instalación
scaphandre --version

# Prueba de medición (10 muestras, intervalo 2 s)
sudo scaphandre json --step 2 --max-top-consumers 5 \
    --timeout 20 > /tmp/test_scaphandre.json
```

---

## 7. Generación de datos TPC-H

```bash
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make

# Generar SF=5 (~5 GB)
./dbgen -s 5

# Verificar archivos generados (esperado: 8 archivos .tbl, total ~5 GB)
ls -lh *.tbl
```

Cargar los archivos `.tbl` a HDFS para Hive:

```bash
hdfs dfs -mkdir -p /tpch
hdfs dfs -put *.tbl /tpch/
hdfs dfs -ls /tpch/
```

---

## 8. Dependencias Python

```bash
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

## 9. Verificación del entorno completo

```bash
# MongoDB clúster sharded
mongosh --port 27017 --eval "sh.status()"

# MongoDB standalone (centralizado)
mongosh --port 27017 --eval "db.adminCommand({ping:1})"

# Hadoop HDFS
hdfs dfsadmin -report

# HiveServer2
beeline -u jdbc:hive2://nodo3:10000 -e "SHOW DATABASES;"

# Scaphandre
sudo scaphandre --version

# Bibliotecas Python
python3 -c "import pymongo, pyhive, pandas, numpy, scipy; print('OK')"
```


# -Mongo-Hive-Environment
