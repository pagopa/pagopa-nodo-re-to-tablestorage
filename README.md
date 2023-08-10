# pagoPA Functions nodo-re-to-tablestorage

Java nodo-re-to-tablestorage Azure Function.
The function aims to dump RE sent via Azure Event Hub to an Azure Table Storage with a TTL of 10 years.

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=pagopa_pagopa-nodo-re-to-tablestorage&metric=alert_status)](https://sonarcloud.io/dashboard?id=pagopa_pagopa-nodo-re-to-tablestorage)


---

## Run locally with Docker
`docker build -t pagopa-functions-nodo-re-to-tablestorage .`

`docker run -it -rm -p 8999:80 pagopa-functions-nodo-re-to-tablestorage`

### Test
`curl http://localhost:8999/example`

## Run locally with Maven

`mvn clean package`

`mvn azure-functions:run`

### Test
`curl http://localhost:7071/example` 

---
