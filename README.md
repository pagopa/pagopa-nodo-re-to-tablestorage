# pagoPA Functions nodo-re-to-datastore

Java nodo-re-to-datastore Azure Function.
The function aims to dump RE sent via Azure Event Hub to a CosmosDB, with a TTL of 120 days, and to an Azure Table Storage with a TTL of 10 years.

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=pagopa_pagopa-nodo-re-to-datastore&metric=alert_status)](https://sonarcloud.io/dashboard?id=pagopa_pagopa-nodo-re-to-datastore)


---

## Run locally with Docker
`docker build -t pagopa-functions-nodo-re-to-datastore .`

`docker run -it -rm -p 8999:80 pagopa-functions-nodo-re-to-datastore`

### Test
`curl http://localhost:8999/example`

## Run locally with Maven

`mvn clean package`

`mvn azure-functions:run`

### Test
`curl http://localhost:7071/example` 

---
