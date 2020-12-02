# Issues identified

## The Debezium Connector

### Out of bound index when adding column
When using version 0.9.5 final, if a new column is added, followed by a new insert using the new row, the connector breaks with an out of bound index error
![error](connector_index_outofbound.png)

This can be fixed by restarting the connector, the new table schema will then be re-scanned and the message will be successfully sent to PubSub.
If the deployment is done using K8 then the healthcheck will fail and a new pod will be created which should fix the issue automatically. 


## The CDC DataFlow Job

### variant overflow when adding column
When a new column is added in the source database, new PubSub messages will contain the new column which breaks the Dataflow pipeline with the `java.lang.RuntimeException: java.io.IOException: varint overflow` error. 
![error](dataflow_variant_overflow.png) 

This cannot be resolved even by restarting the dataflow job, and I am not entirely sure why yet. It is surely related to the added column, and the Dataflow pipeline is not handling it correctly.

Possible solutions
1. Ignore unknown values - we can turn this on and ignore all new columns, this will lose data from the new columns but will allow the pipeline to continue
1. Allow streaming inserts to add new columns somehow (not sure if this works with the Record type which is what's in the changelog table), there might be issues on streaming inserts rejecting the new column when it's just added so needs verifying it works

 
