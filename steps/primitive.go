package steps

import (
	"io"
)

type ByteWriter interface {
	Grow(int)
	WriteByte(byte) error
}

type StringWriter interface {
	WriteString(string) (int, error)
}

func encodeInt(w io.Writer, byteCount int, encoded uint64) error {
	var err error
	var bb []byte
	bw, ok := w.(ByteWriter)
	// To avoid reallocations, grow capacity to the largest possible size
	// for this integer
	if ok {
		bw.Grow(byteCount)
	} else {
		bb = make([]byte, 0, byteCount)
	}

	if encoded == 0 {
		if bw != nil {
			err = bw.WriteByte(0)
			if err != nil {
				return err
			}
		} else {
			bb = append(bb, byte(0))
		}
	} else {
		for encoded > 0 {
			b := byte(encoded & 127)
			encoded = encoded >> 7
			if !(encoded == 0) {
				b |= 128
			}
			if bw != nil {
				err = bw.WriteByte(b)
				if err != nil {
					return err
				}
			} else {
				bb = append(bb, b)
			}
		}
	}
	if bw == nil {
		_, err := w.Write(bb)
		return err
	}
	return nil

}

func readArrayString(r io.Reader) ([]string, error) {
	var err error
	var blkSize int64
	var arr []string
	for {
		blkSize, err = readLong(r)
		if err != nil {
			return nil, err
		}
		if blkSize == 0 {
			break
		}
		if blkSize < 0 {
			blkSize = -blkSize
			_, err = readLong(r)
			if err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < blkSize; i++ {
			elem, err := readString(r)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
	}
	return arr, nil
}

func readDataRecord(r io.Reader) (*DataRecord, error) {
	var str DataRecord
	var err error
	str.Steps, err = readStepsRecordArray(r)
	if err != nil {
		return nil, err
	}

	return &str, nil
}

func readStepsRecord(r io.Reader) (*StepsRecord, error) {
	var str StepsRecord
	var err error
	str.Name, err = readString(r)
	if err != nil {
		return nil, err
	}
	str.Type, err = readString(r)
	if err != nil {
		return nil, err
	}
	str.Jar, err = readString(r)
	if err != nil {
		return nil, err
	}
	str.Arguments, err = readArrayString(r)
	if err != nil {
		return nil, err
	}

	return &str, nil
}

func readPlaybookRecord(r io.Reader) (*PlaybookRecord, error) {
	var str PlaybookRecord
	var err error
	str.Schema, err = readString(r)
	if err != nil {
		return nil, err
	}
	str.Data, err = readDataRecord(r)
	if err != nil {
		return nil, err
	}

	return &str, nil
}

func readLong(r io.Reader) (int64, error) {
	var v uint64
	buf := make([]byte, 1)
	for shift := uint(0); ; shift += 7 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		v |= uint64(b&127) << shift
		if b&128 == 0 {
			break
		}
	}
	datum := (int64(v>>1) ^ -int64(v&1))
	return datum, nil
}

func readString(r io.Reader) (string, error) {
	len, err := readLong(r)
	if err != nil {
		return "", err
	}
	bb := make([]byte, len)
	_, err = io.ReadFull(r, bb)
	if err != nil {
		return "", err
	}
	return string(bb), nil
}

func writeArrayString(r []string, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil {
		return err
	}
	for _, e := range r {
		err = writeString(e, w)
		if err != nil {
			return err
		}
	}
	return writeLong(0, w)
}

func readStepsRecordArray(r io.Reader) ([]*StepsRecord, error) {
	var err error
	var blkSize int64
	var arr []*StepsRecord
	for {
		blkSize, err = readLong(r)
		if err != nil {
			return nil, err
		}
		if blkSize == 0 {
			break
		}
		if blkSize < 0 {
			blkSize = -blkSize
			_, err = readLong(r)
			if err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < blkSize; i++ {
			elem, err := readStepsRecord(r)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
	}
	return arr, nil
}

func writeDataRecord(r *DataRecord, w io.Writer) error {
	var err error
	err = writeStepsRecordArray(r.Steps, w)
	if err != nil {
		return err
	}

	return nil
}
func writeStepsRecord(r *StepsRecord, w io.Writer) error {
	var err error
	err = writeString(r.Name, w)
	if err != nil {
		return err
	}
	err = writeString(r.Type, w)
	if err != nil {
		return err
	}
	err = writeString(r.Jar, w)
	if err != nil {
		return err
	}
	err = writeArrayString(r.Arguments, w)
	if err != nil {
		return err
	}

	return nil
}
func writePlaybookRecord(r *PlaybookRecord, w io.Writer) error {
	var err error
	err = writeString(r.Schema, w)
	if err != nil {
		return err
	}
	err = writeDataRecord(r.Data, w)
	if err != nil {
		return err
	}

	return nil
}

func writeStepsRecordArray(r []*StepsRecord, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil {
		return err
	}
	for _, e := range r {
		err = writeStepsRecord(e, w)
		if err != nil {
			return err
		}
	}
	return writeLong(0, w)
}

func writeLong(r int64, w io.Writer) error {
	downShift := uint64(63)
	encoded := uint64((r << 1) ^ (r >> downShift))
	const maxByteSize = 10
	return encodeInt(w, maxByteSize, encoded)
}

func writeString(r string, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil {
		return err
	}
	if sw, ok := w.(StringWriter); ok {
		_, err = sw.WriteString(r)
	} else {
		_, err = w.Write([]byte(r))
	}
	return err
}
