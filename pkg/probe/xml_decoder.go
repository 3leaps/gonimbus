package probe

import (
	"encoding/xml"
	"io"

	"golang.org/x/net/html/charset"
)

func newXMLDecoder(r io.Reader) *xml.Decoder {
	dec := xml.NewDecoder(r)
	dec.CharsetReader = charset.NewReaderLabel
	return dec
}
