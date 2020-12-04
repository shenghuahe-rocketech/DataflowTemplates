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

This might be where it broke. The beam schema is passed into the decoder, and if the schema does not match (in this case it won't) the message cannot be decoded
![error](decode_issue.png) 

Possible solutions
1. (easier) Ignore unknown values - we can turn this on and ignore all new columns, this will lose data from the new columns but will allow the pipeline to continue
1. (hard) Allow streaming inserts to add new columns somehow (not sure if this works with the Record type which is what's in the changelog table), there might be issues on streaming inserts rejecting the new column when it's just added so needs verifying it works

 
## Progress of solving
Found
- The issue seems to be caused by the connector successfully detects the new schema after restarting (the code does not detect schema changes), but failing
to update the schema in the Data Catalog Entry Group because it already exists. 
- Encoding the message before sending to pubsub isn't causing any issues if the newly added column is not used. Meaning it's either remains null for an existing record, 
or left as null for a new record. These are all considered backwards compatible and does not require schema change. 

Next steps 1
- Find out how to update existing entries in data catalog then we should be able to encode and decode messages with new schema

Progress so far and more issues
- Dropping schema from data catalog if it exist and updating it with the latest one
- Appending is working for log table but not for merge which makes sense because schema has changed

**Deletion risks**
 
Deleting from the end of the table does not cause the connector to break but deleting from the middle is very bad
- when deleting from the middle, an old schema will be used, in the event the data types on remaining columns don't match, the connector will break which is safe
- when deleting from the middle, an old schema will be used, in the event the data types on remaining columns `DO mbatch up`, the data and column will be encoded `incorrectly` and data is now corrupted (i.e.column c goes into b)

This is very hard to resolve and can cause serious issues, the issue is we need to track the schema has changed immediately when it's changed
in order to encode & decode the message correctly but this isn't possible using the current approach because the schema is 
tracked separately in data catalog which isn't going to work, it has to be on each message instead. 

Next steps 2
- See if we can make sure columns added to the data catalog is never removed
- fix merge query
