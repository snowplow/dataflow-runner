package steps

import (
	"io"
)

type StepsRecord struct {
	Name            string
	Type            string
	ActionOnFailure string
	Jar             string
	Arguments       []string
}

func DeserializeStepsRecord(r io.Reader) (*StepsRecord, error) {
	return readStepsRecord(r)
}

func (r StepsRecord) Serialize(w io.Writer) error {
	return writeStepsRecord(&r, w)
}
