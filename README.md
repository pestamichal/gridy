# Instalacja HBase

1. ```git clone https://github.com/big-data-europe/docker-hbase ```

2. ``` cd docker-hbase ```
3. ``` docker-compose -f docker-compose-distributed-local.yml up -d ```

4. W docker-hbase/docker-compose-distributed-local.yml dodajcie mapowanie portu dla serwera Thrift:
``` 
    hbase-master:
    image: bde2020/hbase-master:1.0.0-hbase1.2.6
    container_name: hbase-master
    hostname: hbase-master
    env_file:
      - ./hbase-distributed-local.env
    environment:
      SERVICE_PRECONDITION: "namenode:50070 datanode:50075 zoo:2181"
    ports:
      - 16010:16010
      - 9090:9090       # <---- to tutaj
```

Po zrobieniu ``` docker ps ``` powinniście mieć 
* nodemanager
* resourcemanager
* namenode
* datanode
* historyserver
* zoo
* hbase-master
* hbase-regionserver

# Uruchomienie Thrift server

do komunikacji z klientem pythonowym

```docker exec -d hbase-master hbase thrift start ``` 
lub ``` docker exec -it hbase-master hbase thrift start ``` żeby zobaczyć logi


# GUI
* HDFS NameNode UI: http://localhost:50070
* YARN ResourceManager UI: http://localhost:8088
* HBase Master UI: http://localhost:16010
* Job History Server: http://localhost:8188

# Przykładowy skrypt test.py