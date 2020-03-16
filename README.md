# pam-internalad-indexer

## How to reindex

```
curl -XPOST 'http://localhost:8080/internal/reindex?indexName={indexname}&from={date}'
curl -XPUT 'http://localhost:8080/internal/schedulerlocks?name=fetchFeedAndIndexAds&minutes=10'
curl -XPUT 'http://localhost:8080/internal/aliases?indexName={indexname}'
```

## Rest lastupdated time for feedtask

```
curl 'http://localhost:8080/internal/feedtasks'
curl -XPUT 'http://localhost:8080/internal/feedtasks?name=fetchInternalAds&lastRun=2020-03-14T11:00:00'

```


