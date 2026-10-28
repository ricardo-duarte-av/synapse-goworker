package pdustore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestEveryQueryIsASelect keeps this package read-only by construction, as
// goinbound's store test does. Until write mode, nothing this worker runs may
// write: every SQL constant (named q...) must be a SELECT or a WITH ... SELECT,
// and the only Exec is the SET LOCAL inside StateForGroup's read-only
// transaction. The session is read-only as well (ReadOnly), so this is the
// second of two locks.
func TestEveryQueryIsASelect(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"qDisableSeqscan": true}
	queries, execs := 0, 0
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.ValueSpec:
					for i, id := range n.Names {
						if !strings.HasPrefix(id.Name, "q") || i >= len(n.Values) {
							continue
						}
						lit, ok := n.Values[i].(*ast.BasicLit)
						if !ok || lit.Kind != token.STRING {
							continue
						}
						sql, err := strconv.Unquote(lit.Value)
						if err != nil {
							t.Fatalf("%s: %v", name, err)
						}
						queries++
						head := strings.ToUpper(strings.TrimSpace(sql))
						if allowed[id.Name] {
							if !strings.HasPrefix(head, "SET LOCAL ") {
								t.Errorf("%s: %s was allowed as a SET LOCAL:\n%s", name, id.Name, sql)
							}
							continue
						}
						if !strings.HasPrefix(head, "SELECT") && !strings.HasPrefix(head, "WITH RECURSIVE") && !strings.HasPrefix(head, "WITH ") {
							t.Errorf("%s: %s is not a SELECT:\n%s", name, id.Name, sql)
						}
						for _, w := range []string{"INSERT ", "UPDATE ", "DELETE ", "UPSERT", "TRUNCATE", "DROP ", "ALTER ", "CREATE "} {
							if strings.Contains(head, w) {
								t.Errorf("%s: %s contains %q:\n%s", name, id.Name, w, sql)
							}
						}
					}
				case *ast.CallExpr:
					sel, ok := n.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch sel.Sel.Name {
					case "Exec":
						execs++
						if len(n.Args) < 2 {
							t.Errorf("%s: Exec without a query", fset.Position(n.Pos()))
						} else if id, ok := n.Args[1].(*ast.Ident); !ok || !allowed[id.Name] {
							t.Errorf("%s: Exec of something other than an allowed SET LOCAL", fset.Position(n.Pos()))
						}
					case "CopyFrom", "SendBatch", "Begin":
						t.Errorf("%s: calls %s", fset.Position(n.Pos()), sel.Sel.Name)
					case "BeginTx":
						// Only read-only transactions.
						if len(n.Args) < 2 || !strings.Contains(exprString(n.Args[1]), "ReadOnly") {
							t.Errorf("%s: BeginTx that is not read-only", fset.Position(n.Pos()))
						}
					}
				}
				return true
			})
		}
	}
	if queries < 20 || execs != 1 {
		t.Fatalf("found %d queries and %d Execs; the test is not looking at the code", queries, execs)
	}
}

func exprString(e ast.Expr) string {
	var b strings.Builder
	ast.Inspect(e, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok {
			b.WriteString(id.Name + " ")
		}
		return true
	})
	return b.String()
}
