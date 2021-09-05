package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile        = configFile("ca.pem")
	ServerCert    = configFile("server.pem")
	ServerKey     = configFile("server-key.pem")
	ClientCert    = configFile("root-client.pem")
	ClientKey     = configFile("root-client-key.pem")
	NobodyCert    = configFile("nobody-client.pem")
	NobodyKey     = configFile("nobody-client-key.pem")
	ACLModelFile  = configFile("model.conf")
	ACLPolicyFile = configFile("policy.csv")
)

func configFile(name string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, name)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(homeDir, ".commitlog", name)
}
