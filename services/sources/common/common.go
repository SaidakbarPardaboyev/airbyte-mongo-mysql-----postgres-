package common

import (
	"fmt"
	"io"
	"strings"
	"syncer/core"
	"text/tabwriter"
)

// Field represents one column/field in a table/collection
type Field struct {
	Name          string
	NormType      core.BSONType // normalized cross-DB type
	DestType      string        // explicit destination PostgreSQL type, e.g. "TIMESTAMPTZ"; overrides NormType mapping
	Nullable      bool
	IsPrimary     bool
	IsUnique      bool
	TableName     string // destination table this field belongs to; set by FillTableNames
	IsFKOf        string // if non-empty, this field is a FK referencing this table name
	SeparateTable bool   // if true, this array field spawns its own child table
}

// Table is one table or collection with its discovered fields
type Table struct {
	Name      string
	Namespace string // schema name (MySQL db, Postgres schema, Mongo collection db)
	Fields    []Field
	FieldMap  map[string]Field // fast lookup by name
	Tables    []string         // distinct destination table names; populated by FillTableNames
}

// AddField appends f to Fields and registers it in FieldMap.
func (s *Table) AddField(f Field) {
	if s.FieldMap == nil {
		s.FieldMap = make(map[string]Field)
	}
	s.Fields = append(s.Fields, f)
	s.FieldMap[f.Name] = f
}

// FillTableNames sets TableName on every field and populates Tables with the
// distinct destination table names.
// Fields whose first dot-notation segment is itself an array field (NormType == BSONTypeArray)
// are assigned that segment as their TableName (e.g. "items.id" → "items").
// All other fields are assigned the Table's own Name.
func (s *Table) FillTableNames() {
	arrayRoots := make(map[string]bool)
	for _, f := range s.Fields {
		if f.SeparateTable {
			arrayRoots[f.Name] = true
		}
	}

	seen := make(map[string]bool)
	s.Tables = s.Tables[:0]

	for i, f := range s.Fields {
		dot := -1
		for j := 0; j < len(f.Name); j++ {
			if f.Name[j] == '.' {
				dot = j
				break
			}
		}

		if dot != -1 && arrayRoots[f.Name[:dot]] {
			s.Fields[i].TableName = f.Name[:dot]
		} else if arrayRoots[f.Name] {
			// The array field itself spawns its own table; exclude it from the parent table.
			s.Fields[i].TableName = ""
		} else {
			s.Fields[i].TableName = s.Name
		}
		s.FieldMap[f.Name] = s.Fields[i]

		if s.Fields[i].TableName != "" && !seen[s.Fields[i].TableName] {
			seen[s.Fields[i].TableName] = true
			s.Tables = append(s.Tables, s.Fields[i].TableName)
		}
	}

	// Inject a FK field into every child table referencing the parent.
	// Strip trailing "s" from parent name: "sales" → "sale_id".
	parentName := s.Name
	fkColName := strings.TrimSuffix(parentName, "s") + "_id"
	for _, tableName := range s.Tables {
		if tableName == parentName {
			continue
		}
		s.Fields = append(s.Fields, Field{
			Name:      fkColName,
			NormType:  core.BSONTypeString,
			DestType:  "VARCHAR",
			TableName: tableName,
			IsFKOf:    parentName,
		})
	}
}

// FilterFields returns a new Table containing only the fields whose names are
// in specs, preserving their order. Fields not found in the receiver get a TEXT
// fallback. DestType is set on each field when PgType is non-empty.
func (s *Table) FilterFields(specs []FieldSpec) *Table {
	out := &Table{
		Name:      s.Name,
		Namespace: s.Namespace,
	}

	for _, spec := range specs {
		f, ok := s.FieldMap[spec.Name]
		if !ok {
			continue
		}
		f.DestType = spec.PgType
		f.SeparateTable = spec.SeparateTable
		out.AddField(f)
	}

	return out
}

// DatabaseScheme is the full discovered schema from a source
type DatabaseScheme struct {
	Tables   []*Table
	TableMap map[string]*Table // keyed by "namespace.name"
}

func NewDatabaseScheme() *DatabaseScheme {
	return &DatabaseScheme{TableMap: make(map[string]*Table)}
}

func (c *DatabaseScheme) Add(s *Table) {
	c.Tables = append(c.Tables, s)
	c.TableMap[tableKey(s.Namespace, s.Name)] = s
}

func (c *DatabaseScheme) Get(namespace, name string) (*Table, bool) {
	s, ok := c.TableMap[tableKey(namespace, name)]
	return s, ok
}

func tableKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
}

// PrintCatalog writes a table-formatted summary to w.
func (d *DatabaseScheme) PrintDatabaseScheme(w io.Writer) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STREAM\tFIELD\tNORM TYPE\tNULLABLE\tPK")
	fmt.Fprintln(tw, "──────\t─────\t─────────\t────────\t──")
	for _, s := range d.Tables {
		for i, f := range s.Fields {
			stream := ""
			if i == 0 {
				stream = s.Namespace + "." + s.Name
			}
			nullable := "yes"
			if !f.Nullable {
				nullable = "no"
			}
			pk := ""
			if f.IsPrimary {
				pk = "✓"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
				stream, f.Name, f.NormType, nullable, pk)
		}
	}
	tw.Flush()
}

// FieldSpec pairs a field name with an optional explicit PostgreSQL destination
// type. Leave PgType empty to have the type inferred from the discovered BSON type.
// Set SeparateTable to true to have array fields stored in their own child table;
// otherwise the field is kept as JSONB (or the given PgType) in the parent table.
type FieldSpec struct {
	Name          string
	PgType        string // e.g. "TIMESTAMPTZ", "BIGINT", "JSONB"; empty = auto
	SeparateTable bool   // if true, array field spawns a child table
}
