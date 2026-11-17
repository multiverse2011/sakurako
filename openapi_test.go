package main

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/multiverse2011/sakurako/internal/apidocs"
)

func TestOpenAPIDocumentIsCurrent(t *testing.T) {
	if !bytes.Equal(openAPISpec, apidocs.YAML()) {
		t.Fatal("openapi.yml is out of date; run go generate ./...")
	}
}

func TestOpenAPIDocumentStructure(t *testing.T) {
	var fromJSON map[string]any
	if err := json.Unmarshal(openAPIJSON, &fromJSON); err != nil {
		t.Fatalf("openapi.json: %v", err)
	}

	doc := apidocs.Document()
	if version, _ := doc["openapi"].(string); !strings.HasPrefix(version, "3.") {
		t.Fatalf("openapi version: %q", version)
	}
	info, _ := doc["info"].(map[string]any)
	title, _ := info["title"].(string)
	infoVersion, _ := info["version"].(string)
	if title == "" || infoVersion == "" {
		t.Fatalf("info: %v", doc["info"])
	}
	paths, ok := doc["paths"].(map[string]any)
	if !ok || len(paths) == 0 {
		t.Fatal("paths missing")
	}
	for path, rawItem := range paths {
		if !strings.HasPrefix(path, "/") {
			t.Errorf("path %q does not start with /", path)
		}
		item, ok := rawItem.(map[string]any)
		if !ok {
			t.Errorf("%s: path item is %T", path, rawItem)
			continue
		}
		for method, rawOperation := range item {
			if method != "get" && method != "head" {
				t.Errorf("%s: unexpected method %q", path, method)
				continue
			}
			operation, ok := rawOperation.(map[string]any)
			if !ok {
				t.Errorf("%s %s: operation is %T", method, path, rawOperation)
				continue
			}
			responses, ok := operation["responses"].(map[string]any)
			if !ok || len(responses) == 0 {
				t.Errorf("%s %s: responses missing", method, path)
			}
			for code, rawResponse := range responses {
				response, ok := rawResponse.(map[string]any)
				if !ok {
					t.Errorf("%s %s %s: response is %T", method, path, code, rawResponse)
					continue
				}
				if description, _ := response["description"].(string); description == "" {
					t.Errorf("%s %s %s: missing description", method, path, code)
				}
			}
			parameters, _ := operation["parameters"].([]any)
			for _, rawParameter := range parameters {
				parameter, ok := rawParameter.(map[string]any)
				if !ok {
					t.Errorf("%s %s: parameter is %T", method, path, rawParameter)
					continue
				}
				name, _ := parameter["name"].(string)
				location, _ := parameter["in"].(string)
				if name == "" || location == "" {
					t.Errorf("%s %s: incomplete parameter %v", method, path, parameter)
				}
				if location == "path" && parameter["required"] != true {
					t.Errorf("%s %s: path parameter %q is not required", method, path, name)
				}
			}
		}
	}

	var checkRefs func(any)
	checkRefs = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			if ref, ok := node["$ref"].(string); ok && resolvePointer(doc, ref) == nil {
				t.Errorf("unresolved $ref %q", ref)
			}
			for key, child := range node {
				if key != "$ref" {
					checkRefs(child)
				}
			}
		case []any:
			for _, child := range node {
				checkRefs(child)
			}
		}
	}
	checkRefs(doc)
}

// resolvePointer resolves an internal $ref such as #/components/schemas/Service.
func resolvePointer(root any, ref string) any {
	if !strings.HasPrefix(ref, "#/") {
		return nil
	}
	current := root
	for _, token := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch node := current.(type) {
		case map[string]any:
			current = node[token]
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(node) {
				return nil
			}
			current = node[index]
		default:
			return nil
		}
		if current == nil {
			return nil
		}
	}
	return current
}
