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

Enable reindex-mode by setting the flag INDEXER_REINDEX=true, and set/change INDEXER_REINDEX_INDEXNAME=new_index_name,
also change the groupId to a new group ADLISTENER_REINDEX_GROUP_ID=new_group_id_name.
Set the offset-timestamp INDEXER_REINDEX_MODE_FROM to a timestamp, if you want to reindex from a certain time. 

Redeploy internal-ad-indexer. On restart the app will be in reindex mode, it will spawn another consumer and
start the reindex process. When it is finished, change the groupId to the same as the reindex group, and switch the alias
to point to the new index.
