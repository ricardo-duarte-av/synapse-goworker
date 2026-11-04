package typing

import (
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

func replRequest(eduType string, content map[string]json.RawMessage) replserver.Request {
	return replserver.Request{Args: map[string]string{"edu_type": eduType}, Content: content}
}
