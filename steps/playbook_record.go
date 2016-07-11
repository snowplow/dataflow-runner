package steps

import (
	"io"
)

type PlaybookRecord struct {
	Schema string
	Data   *DataRecord
}

func DeserializePlaybookRecord(r io.Reader) (*PlaybookRecord, error) {
	return readPlaybookRecord(r)
}

func (r PlaybookRecord) Serialize(w io.Writer) error {
	return writePlaybookRecord(&r, w)
}
