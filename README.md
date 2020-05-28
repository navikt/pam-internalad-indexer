![build-deploy-dev](https://github.com/navikt/pam-internalad-indexer/workflows/build-deploy-dev/badge.svg)
![deploy-prod](https://github.com/navikt/pam-internalad-indexer/workflows/deploy-prod/badge.svg)
# pam-internalad-indexer

### Prerequisite
* Elasticsearch
* Kafka with StillingIntern topic

### How to run in dev
```
./gradlew clean build
docker-compose up --build

run Application.java in intellij

```

### How to reindex

Enable reindex-mode by setting the flag INDEXER_REINDEX_MODE_ENABLED=true, and set the INDEXER_REINDEX_MODE_INDEXNAME 
to something meaningful, set the offset-timestamp INDEXER_REINDEX_MODE_FROM to a timestamp, 
this will reset the offset of the reindex-mode to this timestamp. 

Restart internal-ad-indexer (delete pod or deploy). On restart the app will be in reindex mode, it will spawn another consumer and
start the reindex process. When it is finished, pause the main indexing process, 

```
curl -X PUT http://localhost:8080/internal/indexer/pause

```

And then switch alias to point to the new index
```
curl -Z PUT http://localhost:8080/internal/aliases?indexName=theNewIndexName
```

Disable the reindex-mode by setting the flag INDEXER_REINDEX_MODE_ENABLED=false, and restart the pod.
