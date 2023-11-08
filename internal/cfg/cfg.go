package cfg

import (
	"sk/internal/ns"

	"github.com/rs/zerolog/log"
	flag "github.com/spf13/pflag"
)

type Configuration struct {
	URI            string
	NS             ns.Namespace
	ChunkLookup    bool
	SkipIndexBuild bool
	JsonArray      bool
	Verbosity      string
	LogFile        string
	ResultFile     string
}

func Init() Configuration {
	config := Configuration{
		NS: ns.Namespace{},
	}

	flag.StringVar(&config.URI, "uri", "", "sharded cluster connection string")
	flag.StringVar(&config.NS.Database, "db", "", "sharded database name")
	flag.StringVar(&config.NS.Collection, "coll", "", "sharded collection name")
	flag.BoolVar(&config.ChunkLookup, "chunkLookup", true, "whether to send additional queries to lookup chunk info for shard key value")
	flag.BoolVar(&config.SkipIndexBuild, "skipIndexBuild", false, "whether to enforce index exists by calling a createIndex on collection (will no-op if already exists)")
	flag.BoolVar(&config.JsonArray, "jsonArray", false, "whether to write the file as a json array instead of a newline delimited list")
	flag.StringVar(&config.Verbosity, "verbosity", "info", "log level [ error | warn | info | debug | trace ]")
	flag.StringVar(&config.LogFile, "logFile", "", "full path (including file name) where the log file should be stored (default \"stdout\")")
	flag.StringVar(&config.ResultFile, "out", "./out.json", "full path (including file name) where the ouput file should be stored")

	flag.Parse()

	return config
}

func (c *Configuration) Validate() {
	if c.URI == "" {
		flag.Usage()
		log.Fatal().Msg("missing required parameters: --uri")
	}
	if c.NS.Database == "" {
		flag.Usage()
		log.Fatal().Msg("missing required parameters: --db")
	}
	if c.NS.Collection == "" {
		flag.Usage()
		log.Fatal().Msg("missing required parameters: --coll")
	}
}
