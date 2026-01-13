package main

import _ "embed"

//go:embed avro/cluster.avsc
var embeddedClusterSchema []byte

//go:embed avro/playbook.avsc
var embeddedPlaybookSchema []byte
