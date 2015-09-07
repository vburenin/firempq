package common

import (
	"encoding/json"
)

func StructToBinary(s interface{}) []byte {
	data, err := json.Marshal(s)
	if err != nil {
		log.Error("Error serializing data: %s", err)
		return nil
	}
	return data
}

func StructFromBinary(s interface{}, data []byte) error {
	return json.Unmarshal(data, s)
}
