package util

import (
	"encoding/json"
	"log"
)

func StructToBinary(s interface{}) []byte {
	data, err := json.Marshal(s)
	if err != nil {
		log.Println("Error serializing data:", err)
		return nil
	}
	return data
}

func StructFromBinary(s interface{}, data []byte) error {
	return json.Unmarshal(data, s)
}
