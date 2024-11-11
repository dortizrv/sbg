package util

import (
	"bytes"
	"strings"

	xj "github.com/basgys/goxml2json"
)

func ReadXml(xmlString string) (*bytes.Buffer, error) {
	xml := strings.NewReader(xmlString)
	json, err := xj.Convert(xml)
	if err != nil {
		return nil, err
	}

	return json, nil
}
