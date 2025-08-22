package lib

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestExecPlan(t *testing.T) {

	fmt.Println("============= Parse binary plan Start===========")
	binaryPlanStr := "tidb_decode_binary_plan('1B2QCs8dCgxQcm9qZWN0aW9uXzUSuRsKCUhhc2hBZ2dfNhLRGAoNUBkfcDE1ErsUCg5JbmRleExvb2tVcF8xNBKtCQoMU2VsDUMYMTIS5QIKEQUk8MlSYW5nZVNjYW5fMTAhaQy8lrf/eEApDlvHYMKC8j8w3KwQOAJAAkp7CnkKC2RoX2FwcF81NjE3EhdkaF9wcm9tb3RlX3JlZmVyX2RvbWFpbhpRCh1pZHhfeW1kX2tpbmRfZG9tYWluX25vZGVfc2l0ZRIKY3JlYXRlX3ltZBIEa2luZBIGZG9tYWluEgtkb21haW5fbm9kZRIJc2l0ZV9jb2RlUkhyYW5nZTpbMjAyNTA2MjQgMywyMDI1MDYyNCAzXSwgWzIwMjUwARkENCwVGfBSNF0sIGtlZXAgb3JkZXI6ZmFsc2VqWXRpa3ZfdGFzazp7cHJvYyBtYXg6MTAwbXMsIG1pbjowcywgYXZnOiA0MS41bXMsIHA4MDo4MG1zLCBwOTURKihpdGVyczozMTMsIAFLGHM6MTN9cP8RAQQBeBEKMP8BGgEBIUEVCOKLG4BGVQEUUnRlcShkOVQALlpTAQQuczEKCCwgIiF9DCIpLCDiOwAkWhV0aW1lOjY4OQH1OGxvb3BzOjI2NWKeAmNvcCkbGCB7bnVtOiAB4CRtYXg6IDE1Ni40AS4hIxAgMy40NwENJSgMNTQuMgENIR4dKARheEEdNGNfa2V5czogNTAxNDQsASRKFgAIdG90BRYQOiAzMTAJTQETIHdhaXQ6IDEyLgVsgGNvcHJfY2FjaGVfaGl0X3JhdGlvOiAwLjAwLCBidWlsZAW5CF9kdQUaJG46IDg3McK1cywBwMBfZGlzdHNxbF9jb25jdXJyZW5jeTogMX0sIHJwY19pbmZvOntDb3A6e251bV9ycGM6AfgUdG90YWxfJSokNzA0bXN9fWrTAqI9AggzLjElFZ49AggsIHNhgiBkZXRhaWw6IHsJfhhwcm9jZXNzLUUYMjY3ODY4LA2aLhwAOF9zaXplOiAzNTQxNTI3NBEjKXwBNzw4MiwgZ2V0X3NuYXBzaG90CdgQIDIuMjMBp2xyb2Nrc2RiOiB7a2V5X3NraXBwZWRfY291bnQ6BUABdxxibG9jazogezmFESEYMTQsIHJlYRUyCDYxMA0RCGJ5dAGYFC41NiBNQg0UJVMQIDUuNjkhVVo3AwgSxggysAQ8MxLcAQoRVGFibGVSb3dJRImwIDEhllCGdSQCd0ZbAxBKKAomCpKwBARSEEIlBABbTugBCDIyMCEsYQKVJQQyNUmeIegANAUfYQcAMQkqiSUQMTA5NSyNJggyMTla8ABYGgECIa9u2qKEOX5AKVHiDE8Bc+4/MMKpfAxSdWlukicEGGRldmljZV+hdiAsIDEsIDIpLCCaOwAMcGFjax05CCwgM5EoFDIuOTdzLJEoDDk3YrdCKAQIMjE5ZWkUOiA1OTguRVMhJxQgODMzLjhlhoUrBDQ4LS4gOTU6IDE5MS44JVE2KwQQMTIyMjhCKwQMNjI5MUXXRfgMOiA1cwkOiSUEODRpbwRjb7olBAQyNwk5AYRWJQQANQXZGF9leHRyYV+eQAQlEGnDRe4MMTAuN4FBANtSQQRSWQIANgmZqlkC/kMEhUMcNDY2MjE1MzklXgRhbI1DFDMwNzg0NVJDBAw1Mi42IcZyQwQYMTQwNTA4LGZDBBgxODI0Mzk0jSKRWAQwNTZIBAw1OS43PkgEwTMANmpJBBwh1t2RUeLsoTpWAxABQAFaFyWcCDk3MSlr8QkQM2LsAWkOPQnFUrWMBS0MIDcyNQHuQGZldGNoX2hhbmRsZTogNzA46R7FiRQ6IDIyLjVF9kmeBDE2BaKBBQhibGVOVgAYMy4xMXMsIOl0CDcsIDJoAig1fSwgbmV4dDogewFQAF8FnAw6IDk3CXQFFAB0BVoYbG9va3VwXxGGADgBdwAsSiEASHJlc3A6IDgxNC41bXN9cNCllgsqsggcIdUrubHRE6JGMgEIUq4Djq0IjYZMaWQtPkNvbHVtbiMyMywgY2FzZSiatAiJwRQzKSwgMSkZRQA0zkUAADQ6RQAINSwgqsQAAYkZgQA2ljwAtUsZOgA3ljoALTo2dgAAOJY8AEp2AAA5UeMEODaplVnjCA9DbyYLCRRPRkZwrIIyBAIkDrPoHUl2rkApAAUBDPA/MANhNCxSpQJncm91cCBieTox3Ag4LCARCyA5LCBmdW5jczplwgAooVIMaW5jdBUgADM5zg5jCS4sABFDADQdIwA0Nk8AESMANR0jADURIxxmaXJzdHJvdxUmDDYpLT666gFiTAAAN5pMADWEBFoThS8MMS4xNPk7LHCs/rsBIZ+gZV0+i0ZaAQS2AQ7ZC1UzBDE2tu4CXTthNjQsIGFuZHJvaWQsIGlvcz0xmooCFco13SGxLYIhmRELADVS6wBGYAI47A54////////////ARgB')"
	result, err := ParseBinaryPlan(binaryPlanStr)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Convert result to JSON format and print
	jsonBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Printf("JSON marshal error: %v\n", err)
		return
	}
	fmt.Println("Parsed result (JSON):\n%s\n" + string(jsonBytes))

	// Print the execution plan
	plan, err := ParseExecPlan(jsonBytes)
	if err != nil {
		fmt.Printf("Error parsing execution plan: %v\n", err)
		return
	}
	PrintExecPlan(plan)
	fmt.Println("=============TestBinaryPlan End===========")

	// print execution plan detail
	PrintExecPlanDetail(plan)
	fmt.Println("=============TestBinaryPlan Detail End===========")
}
