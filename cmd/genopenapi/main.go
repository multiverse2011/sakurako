// Command genopenapi writes the OpenAPI document from the shared API declarations.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/multiverse2011/sakurako/internal/apidocs"
)

func main() {
	check := flag.Bool("check", false, "fail if the generated document is out of date")
	out := flag.String("out", "openapi.yml", "output path")
	flag.Parse()
	data := apidocs.YAML()
	if *check {
		current, err := os.ReadFile(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if !bytes.Equal(current, data) {
			fmt.Fprintln(os.Stderr, "openapi.yml is out of date; run go generate ./...")
			os.Exit(1)
		}
		return
	}
	if err := os.WriteFile(*out, data, 0644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
