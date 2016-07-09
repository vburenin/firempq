package sns_response

import (
	"bytes"
	"encoding/xml"
	"log"
)

func EncodeXml(doc interface{}) string {
	var b bytes.Buffer
	enc := xml.NewEncoder(&b)
	enc.Indent("", "")
	b.WriteString(`<?xml version="1.0"?>`)
	if err := enc.Encode(doc); err != nil {
		log.Fatal("Could not serialized xml data: " + err.Error())
	}
	return b.String()
}

type SNSResponse interface {
	XmlDocument() string
	HttpCode() int
}
