// Command stripcomments removes comments from Go files while keeping
// compiler directives (//go:embed, //go:build, //go:generate...), legacy
// build tags (// +build) and lint directives (//nolint).
//
//	go run ./tools/stripcomments .
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func isDirective(text string) bool {
	return strings.HasPrefix(text, "//go:") ||
		strings.HasPrefix(text, "// +build") ||
		strings.HasPrefix(text, "//nolint") ||
		strings.HasPrefix(text, "//lint:")
}

func strip(path string) (bool, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return false, err
	}

	var kept []*ast.CommentGroup
	for _, g := range f.Comments {
		var list []*ast.Comment
		for _, c := range g.List {
			if isDirective(c.Text) {
				list = append(list, c)
			}
		}
		if len(list) > 0 {
			kept = append(kept, &ast.CommentGroup{List: list})
		}
	}
	f.Comments = kept
	// Doc fields would otherwise reattach removed comments.
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.File:
			x.Doc = nil
		case *ast.GenDecl:
			x.Doc = nil
		case *ast.FuncDecl:
			x.Doc = nil
		case *ast.Field:
			x.Doc, x.Comment = nil, nil
		case *ast.ValueSpec:
			x.Doc, x.Comment = nil, nil
		case *ast.TypeSpec:
			x.Doc, x.Comment = nil, nil
		case *ast.ImportSpec:
			x.Doc, x.Comment = nil, nil
		}
		return true
	})

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, f); err != nil {
		return false, err
	}
	if bytes.Equal(buf.Bytes(), src) {
		return false, nil
	}
	return true, os.WriteFile(path, buf.Bytes(), 0o644)
}

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	changed := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "vendor" || d.Name() == "tools") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		ok, err := strip(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if ok {
			changed++
			fmt.Println("stripped", path)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("%d files changed\n", changed)
}
