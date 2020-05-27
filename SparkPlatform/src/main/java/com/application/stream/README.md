# spark structured streaming+kafka 流式数据sql处理
-----
## 数据源：rocksdb
## 消息队列中间件：kafka
## 数据处理：structured streaming
### 在structured streaming中通过Append模式进行非聚合查询操作，以Foreach格式汇总每一条查询记录
### 流程图如下
![SparkStructuredStream数据流处理.png](https://i.loli.net/2020/05/20/GQbzPXsmfS7lhMT.png)