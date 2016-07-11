package steps

import (
	"io"
)

type DataRecord struct {
	Steps []*StepsRecord
}

func DeserializeDataRecord(r io.Reader) (*DataRecord, error) {
	return readDataRecord(r)
}

func (r DataRecord) Serialize(w io.Writer) error {
	return writeDataRecord(&r, w)
}
