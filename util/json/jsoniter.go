package json

import jsoniter "github.com/json-iterator/go"

var config = jsoniter.Config{
	EscapeHTML:                    true,
	MarshalFloatWith6Digits:       true,
	ObjectFieldMustBeSimpleString: true,
	UseNumber:                     true,
}.Froze()

var (
	json            = config
	Marshal         = json.Marshal
	Unmarshal       = json.Unmarshal
	NewDecoder      = json.NewDecoder
	NewEncoder      = json.NewEncoder
	MarshalToString = json.MarshalToString
)
