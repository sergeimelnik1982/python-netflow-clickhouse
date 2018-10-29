Simple NETFLOW collector with python and clickhouse support.
Code taken from here and added clickhouse support:
https://github.com/mihird85/flowcollector

To install on clickhouse side create DB:
```
create database NETFLOW
```

and create table:
```
create table IF NOT EXISTS NETFLOW.FLOWS ( EventDate DateTime DEFAULT now(),EventTimestamp UInt64, SrcIp String, DstIp String, Proto UInt8, SrcPort UInt16, DstPort UInt16) ENGINE = MergeTree() PARTITION BY toStartOfMinute(EventDate) ORDER BY(SrcIp,DstIp) SETTINGS index_granularity=8192;
```

install clickhouse driver for python
```
pip3 install clickhouse-driver

run server:
```
python3 pynetflow.py
