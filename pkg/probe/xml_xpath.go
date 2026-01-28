package probe

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

// XMLXPath is a deliberately small XPath-like selector.
//
// Supported forms:
// - //TagName      (match any element with local name TagName)
// - /a/b/c         (match exact element path)
//
// Predicates, attributes, and namespaces are not supported.
type XMLXPath struct {
	anywhere bool
	path     []string
}

func CompileXMLXPath(expr string) (*XMLXPath, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("xpath is empty")
	}
	if strings.Contains(expr, "//") && !strings.HasPrefix(expr, "//") {
		return nil, fmt.Errorf("xpath descendant selector is only supported at the beginning (use //TagName)")
	}

	if strings.HasPrefix(expr, "//") {
		tag := strings.TrimPrefix(expr, "//")
		tag = strings.TrimSpace(tag)
		if tag == "" {
			return nil, fmt.Errorf("xpath // requires a tag")
		}
		if strings.Contains(tag, "/") {
			return nil, fmt.Errorf("xpath // form does not support nested paths")
		}
		return &XMLXPath{anywhere: true, path: []string{tag}}, nil
	}

	if !strings.HasPrefix(expr, "/") {
		return nil, fmt.Errorf("xpath must start with '/' or '//'")
	}

	parts := strings.Split(expr, "/")
	path := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.ContainsAny(p, "[]@") {
			return nil, fmt.Errorf("xpath predicates/attributes not supported: %q", p)
		}
		path = append(path, p)
	}
	if len(path) == 0 {
		return nil, fmt.Errorf("xpath has no path segments")
	}
	return &XMLXPath{path: path}, nil
}

func (x *XMLXPath) FindFirstText(xmlBytes []byte) (string, bool, error) {
	dec := xml.NewDecoder(bytes.NewReader(xmlBytes))

	var stack []string
	for {
		tok, err := dec.Token()
		if err != nil {
			if err == io.EOF {
				return "", false, nil
			}
			return "", false, err
		}

		switch t := tok.(type) {
		case xml.StartElement:
			name := t.Name.Local
			stack = append(stack, name)

			if x.anywhere {
				if name == x.path[0] {
					text, err := readElementText(dec)
					if err != nil {
						return "", false, err
					}
					text = strings.TrimSpace(text)
					if text == "" {
						return "", false, nil
					}
					return text, true, nil
				}
				continue
			}

			if len(stack) == len(x.path) && stackEqual(stack, x.path) {
				text, err := readElementText(dec)
				if err != nil {
					return "", false, err
				}
				text = strings.TrimSpace(text)
				if text == "" {
					return "", false, nil
				}
				return text, true, nil
			}
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
}

func readElementText(dec *xml.Decoder) (string, error) {
	var b strings.Builder
	depth := 1
	for depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			return "", err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		case xml.CharData:
			b.Write([]byte(t))
		}
	}
	return b.String(), nil
}

func stackEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
