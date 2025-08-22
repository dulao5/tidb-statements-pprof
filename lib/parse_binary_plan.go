package lib

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	tipb "github.com/pingcap/tipb/go-tipb"
)

// ParseBinaryPlan Parse TiDB Binary_plan field
// Parameters: binaryPlanStr - string containing tidb_decode_binary_plan() format
// Returns: map[string]interface{} - parsed data dictionary, error - error information
func ParseBinaryPlan(binaryPlanStr string) (map[string]interface{}, error) {
	// Extract base64 encoded part (remove 'tidb_decode_binary_plan(' and ')')
	if !strings.HasPrefix(binaryPlanStr, "tidb_decode_binary_plan('") || !strings.HasSuffix(binaryPlanStr, "')") {
		return nil, fmt.Errorf("invalid Binary_plan format")
	}

	// Extract base64 string
	base64Str := binaryPlanStr[25 : len(binaryPlanStr)-2]

	// Base64 decode
	decodedCompressed, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, fmt.Errorf("base64 decode error: %v", err)
	}

	// Snappy decompress
	decompressedData, err := snappy.Decode(nil, decodedCompressed)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress error: %v", err)
	}

	// Protobuf parse
	explainData := &tipb.ExplainData{}
	err = proto.Unmarshal(decompressedData, explainData)
	if err != nil {
		return nil, fmt.Errorf("protobuf unmarshal error: %v", err)
	}

	// Convert to JSON string, then parse to map[string]interface{}
	marshaler := &jsonpb.Marshaler{}
	jsonStr, err := marshaler.MarshalToString(explainData)
	if err != nil {
		return nil, fmt.Errorf("protobuf to json error: %v", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal([]byte(jsonStr), &result)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal error: %v", err)
	}

	return result, nil
}
