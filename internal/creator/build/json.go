package build

import "encoding/json/v2"

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
