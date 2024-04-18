package config

import (
	"encoding/json"
)

func Load(configRaw []byte, configOut interface{}) error {
	if configRaw == nil {
		return nil
	}
	if len(configRaw) == 0 {
		return nil
	}
	return json.Unmarshal(configRaw, configOut)
}
