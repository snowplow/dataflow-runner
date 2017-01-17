//
// Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Apache License Version 2.0,
// and you may not use this file except in compliance with the Apache License Version 2.0.
// You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the Apache License Version 2.0 is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
//

package main

import (
	"bytes"
	"encoding/json"
	"github.com/elodina/go-avro"
	"os"
	"text/template"
	"time"
)

const (
	CLUSTER_SCHEMA_PATH  = "build/avro/cluster.avsc"
	PLAYBOOK_SCHEMA_PATH = "build/avro/playbook.avsc"
)

var (
	templFuncs = template.FuncMap{
		"nowWithFormat": func(format string) string {
			return time.Now().Format(format)
		},
		"systemEnv": func(env string) string {
			return os.Getenv(env)
		},
	}
)

type SelfDescribingRecord struct {
	Schema string
	Data   interface{}
}

func (sdr SelfDescribingRecord) GetDataByteArray() []byte {
	return []byte(InterfaceToJsonString(sdr.Data, false))
}

type ConfigResolver struct {
	ClusterSchema  avro.Schema
	PlaybookSchema avro.Schema
}

// InitAvroResolver creates a new ConfigResolver instance
func InitConfigResolver() (*ConfigResolver, error) {
	var err error

	// Load Schemas from bindata
	clusterSchemaRaw, err := Asset(CLUSTER_SCHEMA_PATH)
	if err != nil {
		return nil, err
	}
	playbookSchemaRaw, err := Asset(PLAYBOOK_SCHEMA_PATH)
	if err != nil {
		return nil, err
	}

	// Parse and store schemas
	clusterSchema, err := avro.ParseSchema(string(clusterSchemaRaw))
	if err != nil {
		return nil, err
	}
	playbookSchema, err := avro.ParseSchema(string(playbookSchemaRaw))
	if err != nil {
		return nil, err
	}

	return &ConfigResolver{ClusterSchema: clusterSchema, PlaybookSchema: playbookSchema}, nil
}

// --- Class

// ParseClusterRecordFromFile attempts to parse a JSON file to a ClusterConfig
func (cr ConfigResolver) ParseClusterRecordFromFile(filePath string, variables map[string]interface{}) (*ClusterConfig, error) {
	jsonBytes, err := FilePathToByteArray(filePath)
	if err != nil {
		return nil, err
	}

	return cr.ParseClusterRecord(jsonBytes, variables)
}

// ParseClusterRecord attempts to parse a JSON file to a ClusterConfig
func (cr ConfigResolver) ParseClusterRecord(jsonBytes []byte, variables map[string]interface{}) (*ClusterConfig, error) {
	sdr, err := toSelfDescribingRecord(jsonBytes, variables)
	if err != nil {
		return nil, err
	}

	// Unmarshall data component to generated type
	dataBytes := sdr.GetDataByteArray()

	recordJson := new(ClusterConfig)
	err1 := parseRecordAsJson(dataBytes, recordJson)
	if err1 != nil {
		return nil, err1
	}

	// Write and decode record as Avro
	decodedRecord := new(ClusterConfig)
	err2 := parseRecordAsAvro(cr.ClusterSchema, recordJson, decodedRecord)
	if err2 != nil {
		return nil, err2
	}

	return decodedRecord, nil
}

// ParsePlaybookRecordFromFile attempts to parse a JSON file to a PlaybookConfig
func (cr ConfigResolver) ParsePlaybookRecordFromFile(filePath string, variables map[string]interface{}) (*PlaybookConfig, error) {
	jsonBytes, err := FilePathToByteArray(filePath)
	if err != nil {
		return nil, err
	}

	return cr.ParsePlaybookRecord(jsonBytes, variables)
}

// ParsePlaybookRecord attempts to parse a JSON file to a PlaybookConfig
func (cr ConfigResolver) ParsePlaybookRecord(jsonBytes []byte, variables map[string]interface{}) (*PlaybookConfig, error) {
	sdr, err := toSelfDescribingRecord(jsonBytes, variables)
	if err != nil {
		return nil, err
	}

	// Unmarshall data component to generated type
	dataBytes := sdr.GetDataByteArray()

	recordJson := new(PlaybookConfig)
	err1 := parseRecordAsJson(dataBytes, recordJson)
	if err1 != nil {
		return nil, err1
	}

	// Write and decode record as Avro
	decodedRecord := new(PlaybookConfig)
	err2 := parseRecordAsAvro(cr.PlaybookSchema, recordJson, decodedRecord)
	if err2 != nil {
		return nil, err2
	}

	return decodedRecord, nil
}

// --- Static

// parseRecordAsJson unmarshalles a byte array to an interface
func parseRecordAsJson(recordBytes []byte, recordJson interface{}) error {
	return json.Unmarshal(recordBytes, &recordJson)
}

// parseRecord writes an unmarshalled version of our record to an Avro writer
// and then decodes it to ensure it is valid.
//
// TODO: When reading JSON is supported remove the writer section
func parseRecordAsAvro(schema avro.Schema, recordJson interface{}, decodedRecord interface{}) error {
	// Write Unmarshalled record using Avro writer
	writer := avro.NewSpecificDatumWriter()
	writer.SetSchema(schema)

	buffer := new(bytes.Buffer)
	encoder := avro.NewBinaryEncoder(buffer)

	writer.Write(recordJson, encoder)

	// Read and decode record using Avro reader
	reader := avro.NewSpecificDatumReader()
	reader.SetSchema(schema)

	decoder := avro.NewBinaryDecoder(buffer.Bytes())

	return reader.Read(decodedRecord, decoder)
}

// toSelfDescribingRecord takes a byte array and returns a SelfDescribingRecord
func toSelfDescribingRecord(jsonBytes []byte, variables map[string]interface{}) (*SelfDescribingRecord, error) {
	templateBytes, err := templateRawBytes(jsonBytes, variables)
	if err != nil {
		return nil, err
	}

	recordJson := new(SelfDescribingRecord)
	err1 := json.Unmarshal(templateBytes, &recordJson)
	if err1 != nil {
		return nil, err1
	}

	return recordJson, nil
}

// templateRawBytes runs the raw config through the golang templater
func templateRawBytes(rawBytes []byte, variables map[string]interface{}) ([]byte, error) {
	t, err := template.New("playbook").Funcs(templFuncs).Parse(string(rawBytes))
	if err != nil {
		return nil, err
	}

	var filled bytes.Buffer
	if err := t.Execute(&filled, variables); err != nil {
		return nil, err
	}

	return filled.Bytes(), nil
}
