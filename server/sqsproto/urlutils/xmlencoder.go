package urlutils

import (
	"bytes"
	"encoding/xml"
	"log"
)

func EncodeXmlDocument(doc interface{}) string {
	var b bytes.Buffer
	enc := xml.NewEncoder(&b)
	enc.Indent("", " ")
	b.WriteString(`<?xml version="1.0"?>` + "\n")
	if err := enc.Encode(doc); err != nil {
		log.Fatal("Could not serialized xml data: " + err.Error())
	}
	return b.String()
}
