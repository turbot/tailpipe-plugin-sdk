# tailpipe-plugin-sdk

## Plugin GRPC Interface

### AddObserver
Returns a stream used by the plugin to send status/progress events


## Collect
- Tell the plugin to start collection

- NOTE: The plugin will execute collection asyncronously, i.e. this call will return immediately and the collection status
  will be updated via the event streeam returned from `AddObserver`

- The plugin sends an event on start and completions (progress events tbd)
- The plugin will rows in chunks to JSONL files in the specified directory. The filename will start with the execution id and end with a sequence number
- the complete event will contain the number of files written - then when collection is complete the plugin manager will
  ensure it has processed all files _for that execution id_)  




