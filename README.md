# ShardKeyValues

## **Warning**
This is a fairly expensive script to run, use at your own caution

## Usage
Usage of ./skv:
      --chunkLookup        whether to send additional queries to lookup chunk info for shard key value (default true)
      --coll string        sharded collection name
      --db string          sharded database name
      --jsonArray          whether to write the file as a json array instead of a newline delimited list
      --logFile string     full path (including file name) where the log file should be stored (default "stdout")
      --out string         full path (including file name) where the ouput file should be stored (default "./out.json")
      --rm                 whether to remove existing result file, if false will attempt to rename an existing result file
      --skipIndexBuild     whether to enforce index exists by calling a createIndex on collection (will no-op if already exists)
      --uri string         sharded cluster connection string
      --verbosity string   log level [ error | warn | info | debug | trace ] (default "info")


## TODO
[] cache chunks to alleviate repetitive queries to config.chunks
[] cleanup code, I'm positive things are not done as efficiently as possible