// Package apidocs defines the public API contract and generates its OpenAPI document.
// Edit the endpoint and schema declarations here, then run go generate ./....
package apidocs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type object = map[string]any

// operation describes one HTTP method's contract.
type operation struct {
	summary     string
	description string
	parameters  []any
	responses   object
}

// endpoint pairs a path with its GET and HEAD contracts so neither can silently lack a field.
type endpoint struct {
	path string
	tags []any
	get  operation
	head operation
}

// Document is shared by the generator and the generated-file freshness test.
func Document() map[string]any {
	paths := object{}
	for _, e := range endpoints() {
		paths[e.path] = object{
			"get":  operationObject(e.tags, e.get),
			"head": operationObject(e.tags, e.head),
		}
	}
	return object{
		"openapi":    "3.0.3",
		"info":       object{"title": "Sakurako API", "version": "1.0.0", "description": "Read-only Mirakurun aggregation API. Catalog reads use synchronized snapshots. GET and HEAD are supported; other methods return 405. Unsupported endpoints return 404. Authentication, when required, is provided by the deployment proxy."},
		"servers":    []any{object{"url": "/"}},
		"tags":       []any{object{"name": "Catalog"}, object{"name": "Streaming"}, object{"name": "Monitoring"}},
		"paths":      paths,
		"components": object{"schemas": schemas()},
	}
}

func operationObject(tags []any, o operation) object {
	out := object{"tags": tags, "summary": o.summary, "description": o.description, "responses": o.responses}
	if o.parameters != nil {
		out["parameters"] = o.parameters
	}
	return out
}

func logoResponses(head bool) object {
	r := errorResponses("400", "404", "409")
	r["405"] = errorResponse("Method not allowed; use GET or HEAD")
	if head {
		r["200"] = object{"description": "Headers only; no body"}
	} else {
		r["200"] = object{"description": "Logo image; the upstream Content-Type is passed through (image/png only when absent).", "content": object{"*/*": object{"schema": object{"type": "string", "format": "binary"}}}}
	}
	r["503"] = errorResponse("No source could provide the logo")
	return r
}

func streamResponse() object {
	return object{"description": "Live transport stream; continues until disconnected.", "headers": object{"X-Mirakurun-Tuner-User-ID": object{"description": "Present when returned by upstream; shared among subscribers.", "schema": object{"type": "string"}}}, "content": object{"video/MP2T": object{"schema": object{"type": "string", "format": "binary"}}}}
}

func endpoints() []endpoint {
	catalog, streaming, monitoring := []any{"Catalog"}, []any{"Streaming"}, []any{"Monitoring"}
	pathID := func() object {
		return object{"name": "id", "in": "path", "required": true, "schema": object{"type": "integer", "format": "int64"}, "description": "Mirakurun numeric identifier"}
	}
	filter := func(name string) object {
		return object{"name": name, "in": "query", "schema": object{"type": "integer", "minimum": 0, "maximum": 65535}, "description": "Exact match; combined with other filters using AND. One value only."}
	}
	filters := []any{filter("networkId"), filter("serviceId"), filter("eventId")}
	programParams := append([]any{pathID()}, filters...)
	arrayOf := func(schema string) object {
		return object{"type": "array", "items": object{"$ref": "#/components/schemas/" + schema}}
	}
	success := func(schema object) object {
		return object{"description": "Success", "content": object{"application/json": object{"schema": schema}}}
	}
	streamParams := func(head bool) []any {
		decode, priority := "Forwarded to Mirakurun; one value only.", "Tuner allocation priority; distinct from backend source priority."
		if head {
			decode, priority = "Validated for compatibility; HEAD allocates no tuner.", "Validated for compatibility; HEAD allocates no tuner."
		}
		return []any{
			pathID(),
			object{"name": "decode", "in": "query", "schema": object{"type": "string", "enum": []any{"0", "1"}}, "description": decode},
			object{"name": "X-Mirakurun-Priority", "in": "header", "schema": object{"type": "integer", "minimum": 0, "maximum": 2147483647}, "description": priority},
		}
	}
	return []endpoint{
		{
			path: "/api/services",
			tags: catalog,
			get: operation{
				summary:     "List services",
				description: "Merged catalog. Conflicting service identities are excluded; unknown upstream fields are preserved.",
				responses:   getResponses(success(arrayOf("Service")), "400"),
			},
			head: operation{
				summary:     "List services (headers only)",
				description: "Resolves the same merged catalog as GET without returning a body.",
				responses:   headResponses("400"),
			},
		},
		{
			path: "/api/services/{id}",
			tags: catalog,
			get: operation{
				summary:     "Get a service",
				description: "",
				parameters:  []any{pathID()},
				responses:   getResponses(success(object{"$ref": "#/components/schemas/Service"}), "400", "404", "409"),
			},
			head: operation{
				summary:     "Get a service (headers only)",
				description: "Resolves the same service as GET without returning a body.",
				parameters:  []any{pathID()},
				responses:   headResponses("400", "404", "409"),
			},
		},
		{
			path: "/api/services/{id}/logo",
			tags: catalog,
			get: operation{
				summary:     "Get a service logo",
				description: "Proxies the preferred source's logo image, falling back to other redundant sources. Bytes and cache headers come from upstream; 404 means no source has a logo.",
				parameters:  []any{pathID()},
				responses:   logoResponses(false),
			},
			head: operation{
				summary:     "Get a service logo (headers only)",
				description: "Returns the same headers as GET without the image bytes.",
				parameters:  []any{pathID()},
				responses:   logoResponses(true),
			},
		},
		{
			path: "/api/channels",
			tags: catalog,
			get: operation{
				summary:     "List virtual channels",
				description: "Channels from the merged catalog. With the default virtual names each channel is scoped to a backend; with upstream names, backends that share a name are collapsed to the configured-priority winner. When those sources disagree on networkId, only services with the winner's networkId are listed. Channel detail is unsupported.",
				responses:   getResponses(success(arrayOf("Channel")), "400"),
			},
			head: operation{
				summary:     "List virtual channels (headers only)",
				description: "Resolves the same virtual channels as GET without returning a body.",
				responses:   headResponses("400"),
			},
		},
		{
			path: "/api/programs",
			tags: catalog,
			get: operation{
				summary:     "List programs",
				description: "Programs from the representative source selected per service. Only the documented query filters are accepted.",
				parameters:  filters,
				responses:   getResponses(success(arrayOf("Program")), "400"),
			},
			head: operation{
				summary:     "List programs (headers only)",
				description: "Applies the same query filters as GET without returning a body.",
				parameters:  filters,
				responses:   headResponses("400"),
			},
		},
		{
			path: "/api/programs/{id}",
			tags: catalog,
			get: operation{
				summary:     "Get a program",
				description: "",
				parameters:  programParams,
				responses:   getResponses(success(object{"$ref": "#/components/schemas/Program"}), "400", "404"),
			},
			head: operation{
				summary:     "Get a program (headers only)",
				description: "Resolves the same program as GET without returning a body.",
				parameters:  programParams,
				responses:   headResponses("400", "404"),
			},
		},
		{
			path: "/api/services/{id}/stream",
			tags: streaming,
			get: operation{
				summary:     "Stream a service",
				description: "Continuous MPEG-TS. Matching service/domain/broadcast type requests share one upstream regardless of tuner priority; the connection keeps the allocation of the request that started it. A decode=0 request may join an existing decode=1 stream, never the reverse. Omitted decode is distinct from an explicit value; an omitted priority is not sent upstream. Source failover is attempted only before sending the first body byte. Slow subscribers may be disconnected independently.",
				parameters:  streamParams(false),
				responses:   getResponses(streamResponse(), "400", "404", "409", "502", "503"),
			},
			head: operation{
				summary:     "Check stream eligibility (headers only)",
				description: "Checks catalog membership and source eligibility without allocating a tuner. Does not verify actual tuner availability or start a stream.",
				parameters:  streamParams(true),
				responses:   headResponses("400", "404", "409", "503"),
			},
		},
		{
			path: "/api/channels/{type}/{channel}/stream",
			tags: streaming,
			get: operation{
				summary:     "Stream a channel",
				description: "Whole-transponder MPEG-TS from the preferred backend for this channel. Channel names follow channel_names (virtual by default, upstream for Mirakurun-compatible clients). With upstream names, configured priority (not health or freshness) picks the broadcast for a shared name, and only sources with the same service domain fail over. Sources that disagree on networkId are different broadcasts: the winner is chosen including stale catalogs, and only fresh sources with the winner's networkId are served, so an unavailable winner never switches to another broadcast. Sharing otherwise matches service streams; priority does not split groups and decode=0 may join a decoded stream.",
				parameters: []any{
					object{"name": "type", "in": "path", "required": true, "schema": object{"type": "string"}, "description": "Broadcast type: GR, BS, CS or SKY"},
					object{"name": "channel", "in": "path", "required": true, "schema": object{"type": "string"}, "description": "Channel name as returned by /api/channels"},
					object{"name": "decode", "in": "query", "schema": object{"type": "string", "enum": []any{"0", "1"}}, "description": "Forwarded to Mirakurun; one value only."},
					object{"name": "X-Mirakurun-Priority", "in": "header", "schema": object{"type": "integer", "minimum": 0, "maximum": 2147483647}, "description": "Tuner allocation priority; distinct from backend source priority."},
				},
				responses: getResponses(streamResponse(), "400", "404", "502", "503"),
			},
			head: operation{
				summary:     "Check channel stream eligibility (headers only)",
				description: "Checks that the channel exists and at least one source is usable without allocating a tuner.",
				parameters: []any{
					object{"name": "type", "in": "path", "required": true, "schema": object{"type": "string"}},
					object{"name": "channel", "in": "path", "required": true, "schema": object{"type": "string"}},
					object{"name": "decode", "in": "query", "schema": object{"type": "string", "enum": []any{"0", "1"}}, "description": "Validated for compatibility; HEAD allocates no tuner."},
					object{"name": "X-Mirakurun-Priority", "in": "header", "schema": object{"type": "integer", "minimum": 0, "maximum": 2147483647}, "description": "Validated for compatibility; HEAD allocates no tuner."},
				},
				responses: headResponses("400", "404", "503"),
			},
		},
		{
			path: "/api/tuners",
			tags: monitoring,
			get: operation{
				summary:     "List tuners",
				description: "Live aggregated tuner state from every backend. Channel names in users' stream settings follow channel_names. With multiple backends, index values are scoped (10000 per backend) and names are prefixed with the backend ID. Backends that fail are omitted and logged; 503 only when every backend fails.",
				responses:   getResponsesWith(success(arrayOf("Tuner")), tunerResponseOverrides, "400", "503"),
			},
			head: operation{
				summary:     "List tuners (headers only)",
				description: "Fetches the same live tuner state as GET without returning a body. Failed backends are omitted and logged.",
				responses:   headResponsesWith(tunerResponseOverrides, "400", "503"),
			},
		},
		{
			path: "/api/events/stream",
			tags: monitoring,
			get: operation{
				summary:     "Tuner event stream",
				description: "Tuner events relayed from every backend in Mirakurun's format: an unclosed JSON array of {resource, type, data, time} objects. Tuner data is rewritten like /api/tuners. Tuner changes arrive as they happen; only while a backend subscription is down is a newline keepalive written every 15 seconds. Cache-Control: no-store.",
				parameters:  []any{object{"name": "resource", "in": "query", "required": true, "schema": object{"type": "string", "enum": []any{"tuner"}}, "description": "Only resource=tuner is supported."}},
				responses:   getResponses(object{"description": "Event stream; the JSON array stays open and never closes.", "content": object{"application/json": object{"schema": object{"type": "string"}}}}, "400"),
			},
			head: operation{
				summary:     "Check event stream availability (headers only)",
				description: "Returns the event stream headers without keeping the connection open.",
				parameters:  []any{object{"name": "resource", "in": "query", "required": true, "schema": object{"type": "string", "enum": []any{"tuner"}}}},
				responses:   headResponses("400"),
			},
		},
		{
			path: "/healthz",
			tags: monitoring,
			get: operation{
				summary:     "Read readiness",
				description: "200 when at least one backend is available and there are no conflicting service IDs or channel names. Partial outages set degraded=true.",
				responses:   healthResponses(false),
			},
			head: operation{
				summary:     "Read readiness (headers only)",
				description: "Applies the same readiness rules as GET without returning a body.",
				responses:   healthResponses(true),
			},
		},
		{
			path: "/-/ui/state",
			tags: monitoring,
			get: operation{
				summary:     "Read UI snapshot",
				description: "Sakurako-specific snapshot of backends, catalog and shared streams. Counters are process-local. Timestamps use RFC 3339; unset timestamps are null. Cache-Control: no-store.",
				responses:   getResponses(success(object{"$ref": "#/components/schemas/UIState"}), "400"),
			},
			head: operation{
				summary:     "Read UI snapshot (headers only)",
				description: "Returns the same snapshot headers as GET without a body.",
				responses:   headResponses("400"),
			},
		},
	}
}

func schemas() object {
	return object{"Tuner": object{"type": "object", "properties": object{"index": object{"type": "integer"}, "name": object{"type": "string"}, "types": object{"type": "array", "items": object{"type": "string"}}, "pid": object{"type": "integer", "nullable": true}, "users": object{"type": "array", "items": object{"type": "object", "additionalProperties": true}}, "isAvailable": object{"type": "boolean"}, "isFault": object{"type": "boolean"}}, "additionalProperties": true}, "Error": object{"type": "object", "properties": object{"code": object{"type": "integer"}, "reason": object{"type": "string"}}, "additionalProperties": true}, "Channel": object{"type": "object", "properties": object{"type": object{"type": "string"}, "channel": object{"type": "string"}, "name": object{"type": "string"}, "services": object{"type": "array", "items": object{"type": "object", "properties": object{"id": object{"type": "integer", "format": "int64"}, "serviceId": object{"type": "integer", "format": "int64"}, "name": object{"type": "string"}}, "additionalProperties": true}}}, "additionalProperties": true}, "Service": object{"type": "object", "properties": object{"id": object{"type": "integer", "format": "int64"}, "networkId": object{"type": "integer", "format": "int64"}, "serviceId": object{"type": "integer", "format": "int64"}, "name": object{"type": "string"}, "type": object{"type": "integer"}, "channel": object{"$ref": "#/components/schemas/Channel"}, "hasLogoData": object{"type": "boolean"}}, "additionalProperties": true}, "Program": object{"type": "object", "properties": object{"id": object{"type": "integer", "format": "int64"}, "networkId": object{"type": "integer", "format": "int64"}, "serviceId": object{"type": "integer", "format": "int64"}, "eventId": object{"type": "integer", "format": "int64"}, "name": object{"type": "string"}, "description": object{"type": "string"}, "startAt": object{"type": "integer", "format": "int64", "description": "Unix epoch milliseconds"}, "duration": object{"type": "integer", "format": "int64", "description": "Milliseconds"}}, "additionalProperties": true}, "HealthBackend": object{"type": "object", "properties": object{"id": object{"type": "string"}, "healthy": object{"type": "boolean"}, "catalog_available": object{"type": "boolean"}, "last_success": object{"type": "string", "format": "date-time", "nullable": true}, "error": object{"type": "string", "nullable": true, "description": "Stable category \"catalog sync failed\" when the last sync failed; null otherwise. Details are logged, not returned."}, "stream_retry_after": object{"type": "string", "format": "date-time", "nullable": true}}, "additionalProperties": true}, "Backend": object{"type": "object", "properties": object{"id": object{"type": "string"}, "ui_url": object{"type": "string", "description": "Mirakurun UI base URL derived from the backend API base."}, "catalog_state": object{"type": "string", "enum": []any{"Healthy", "Using cached catalog", "Expired", "Not synced"}, "description": "Not synced (never succeeded), Expired (older than max_stale_seconds), Using cached catalog (fresh but last sync failed), or Healthy."}, "catalog_available": object{"type": "boolean"}, "healthy": object{"type": "boolean"}, "available": object{"type": "boolean"}, "last_success": object{"type": "string", "format": "date-time", "nullable": true}, "retry_after": object{"type": "string", "format": "date-time", "nullable": true}, "domain": object{"type": "string", "description": "Service identity domain: shared:<service_domain> when configured, otherwise backend:<id>."}, "priority": object{"type": "integer"}}, "additionalProperties": true}, "Client": object{"type": "object", "properties": object{"id": object{"type": "string"}, "remote": object{"type": "string"}, "user_agent": object{"type": "string"}, "joined": object{"type": "string", "format": "date-time"}, "bytes": object{"type": "integer", "format": "int64"}}, "additionalProperties": true}, "Stream": object{"type": "object", "properties": object{"id": object{"type": "string"}, "service_id": object{"type": "integer", "format": "int64", "description": "0 for channel streams."}, "service_name": object{"type": "string"}, "channel": object{"type": "string", "description": "Exposed channel name; empty for service streams."}, "channel_type": object{"type": "string"}, "domain": object{"type": "string", "description": "Service identity domain: shared:<service_domain> when configured, otherwise backend:<id>."}, "decode": object{"type": "string", "description": "Empty when the client did not specify decode."}, "priority": object{"type": "string", "description": "Empty when the client did not specify a priority."}, "state": object{"type": "string", "enum": []any{"starting", "streaming"}}, "backend_id": object{"type": "string"}, "created": object{"type": "string", "format": "date-time"}, "started": object{"type": "string", "format": "date-time", "nullable": true}, "upstream_bytes": object{"type": "integer", "format": "int64"}, "clients": object{"type": "array", "items": object{"$ref": "#/components/schemas/Client"}}}, "additionalProperties": true}, "CatalogService": object{"type": "object", "properties": object{"id": object{"type": "integer", "format": "int64"}, "name": object{"type": "string"}, "type": object{"type": "string", "nullable": true}, "preferred": object{"type": "string"}, "candidates": object{"type": "array", "items": object{"type": "object", "properties": object{"id": object{"type": "string"}, "available": object{"type": "boolean"}}, "additionalProperties": true}}}, "additionalProperties": true}, "Health": object{"type": "object", "properties": object{"ready": object{"type": "boolean"}, "degraded": object{"type": "boolean"}, "backends": object{"type": "array", "items": object{"$ref": "#/components/schemas/HealthBackend"}}, "conflicting_service_ids": object{"type": "array", "items": object{"type": "integer", "format": "int64"}}, "conflicting_channels": object{"type": "array", "items": object{"type": "string"}, "description": "Exposed channel keys (type/channel) whose upstream sources disagree on networkId. Only sources with the priority winner's networkId are used for these channels, so a stale winner never fails over to another broadcast. 503 when non-empty."}}, "additionalProperties": true}, "UIState": object{"type": "object", "properties": object{"observed_at": object{"type": "string", "format": "date-time"}, "downstream_bytes": object{"type": "integer", "format": "int64"}, "sync_seconds": object{"type": "integer"}, "backends": object{"type": "array", "items": object{"$ref": "#/components/schemas/Backend"}}, "services": object{"type": "array", "items": object{"$ref": "#/components/schemas/CatalogService"}}, "streams": object{"type": "array", "items": object{"$ref": "#/components/schemas/Stream"}}, "conflicting_service_ids": object{"type": "array", "items": object{"type": "integer", "format": "int64"}}}, "additionalProperties": true}}
}

var responseDescriptions = map[string]string{
	"400": "Invalid ID, query or header",
	"404": "Not found or unsupported endpoint",
	"409": "Service identity conflict",
	"502": "Upstream refused the stream",
	"503": "No healthy source could start the stream",
}

// tunerResponseOverrides replaces the shared texts that do not fit /api/tuners.
var tunerResponseOverrides = map[string]string{
	"400": "Query options are not supported here",
	"503": "Tuner state unavailable from all backends",
}

// errorResponses fails loudly instead of emitting a response with no description.
func errorResponses(codes ...string) object {
	return errorResponsesWith(nil, codes...)
}

func errorResponsesWith(overrides map[string]string, codes ...string) object {
	result := object{}
	for _, code := range codes {
		description, ok := overrides[code]
		if !ok {
			description, ok = responseDescriptions[code]
		}
		if !ok {
			panic(fmt.Sprintf("apidocs: no description for response code %q", code))
		}
		result[code] = errorResponse(description)
	}
	return result
}

func getResponses(success object, codes ...string) object {
	return getResponsesWith(success, nil, codes...)
}

func getResponsesWith(success object, overrides map[string]string, codes ...string) object {
	result := errorResponsesWith(overrides, codes...)
	result["200"] = success
	result["405"] = errorResponse("Method not allowed; use GET or HEAD")
	return result
}

func headResponses(codes ...string) object {
	return headResponsesWith(nil, codes...)
}

func headResponsesWith(overrides map[string]string, codes ...string) object {
	result := errorResponsesWith(overrides, codes...)
	result["200"] = object{"description": "Headers only; no body"}
	return result
}

func errorResponse(description string) object {
	return object{"description": description, "content": object{"application/json": object{"schema": object{"$ref": "#/components/schemas/Error"}}}}
}

func healthResponses(head bool) object {
	health := object{"$ref": "#/components/schemas/Health"}
	badRequest := errorResponse(responseDescriptions["400"])
	if head {
		return object{"200": object{"description": "Headers only; no body"}, "400": badRequest, "503": object{"description": "Not ready"}}
	}
	return object{
		"200": object{"description": "Success", "content": object{"application/json": object{"schema": health}}},
		"400": badRequest,
		"405": errorResponse("Method not allowed; use GET or HEAD"),
		"503": object{"description": "Not ready", "content": object{"application/json": object{"schema": health}}},
	}
}

// YAML uses JSON-quoted scalars (valid YAML) and sorted mapping keys for reproducibility.
// This deliberately small writer supports only the types used by Document.
func YAML() []byte {
	var b bytes.Buffer
	b.WriteString("# Code generated by go generate ./...; DO NOT EDIT.\n# Source: internal/apidocs/spec.go\n")
	writeYAML(&b, Document(), 0)
	return b.Bytes()
}
func writeYAML(b *bytes.Buffer, value any, indent int) {
	pad := strings.Repeat(" ", indent)
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(b, "%s%s:", pad, scalar(key))
			writeValue(b, v[key], indent)
		}
	case []any:
		for _, item := range v {
			fmt.Fprintf(b, "%s-", pad)
			writeValue(b, item, indent)
		}
	default:
		panic(fmt.Sprintf("unsupported YAML container %T", value))
	}
}
func writeValue(b *bytes.Buffer, v any, indent int) {
	nested := false
	switch x := v.(type) {
	case map[string]any:
		nested = len(x) > 0
	case []any:
		nested = len(x) > 0
	}
	if nested {
		b.WriteByte('\n')
		writeYAML(b, v, indent+2)
	} else {
		fmt.Fprintf(b, " %s\n", scalar(v))
	}
}
func scalar(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}
