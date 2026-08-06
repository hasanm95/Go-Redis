package config

import "flag"

type Config struct {
	Mode string
	Port string
	MasterAddr string
}

func SetupFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.Mode, "mode", "master", "server mode")
	flag.StringVar(&cfg.Port, "port", "6380", "server port")
	flag.StringVar(&cfg.MasterAddr, "masterAddr", "", "")

	flag.Parse()

	return cfg
}
