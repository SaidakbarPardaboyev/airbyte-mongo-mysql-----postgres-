package sync_common

import (
	"airbyte-service/core"
	"strings"
)

type Field struct {
	Name            string
	SourceType      core.BSONType
	DestinationType string
	Nullable        bool
	IsPrimary       bool
	TableName       string
	IsChildTable    bool
}

type Table struct {
	Name             string
	Database         string
	WriteMode        core.WriteMode
	Fields           []Field
	FieldMap         map[string]Field
	Tables           []string
	CreatedTimeField string
	UpdatedTimeField string
	DeletedTimeField string
}

func MongoToPostgresFieldName(name string) string {
	if name == "_id" {
		return "id"
	}
	return name
}

func (s *Table) AddField(f Field) {
	if s.FieldMap == nil {
		s.FieldMap = make(map[string]Field)
	}
	s.Fields = append(s.Fields, f)
	s.FieldMap[f.Name] = f
}

func (s *Table) FillTableNames() {
	// create map for know which field is child table names
	childTableNamesMap := make(map[string]bool)
	for _, f := range s.Fields {
		if f.IsChildTable {
			childTableNamesMap[f.Name] = true
		}
	}

	// seen is to take all unique table names
	seen := make(map[string]bool)
	s.Tables = []string{}

	// add table names to field's object
	for i, f := range s.Fields {
		dotIndx := strings.Index(f.Name, ".")

		// if the field is belong to inner table, add the table name to field's TableName
		if dotIndx != -1 && childTableNamesMap[f.Name[:dotIndx]] {
			s.Fields[i].TableName = f.Name[:dotIndx]
			// if the field is not the name of innner table or not belong to inner table, just put the parent table name
		} else if !childTableNamesMap[f.Name] {
			s.Fields[i].TableName = s.Name
		}

		// fill out field map
		s.FieldMap[f.Name] = s.Fields[i]

		// if tha table did not added to table list, just add it
		if s.Fields[i].TableName != "" && !seen[s.Fields[i].TableName] {
			seen[s.Fields[i].TableName] = true
			s.Tables = append(s.Tables, s.Fields[i].TableName)
		}
	}

	// add parent table id field to inner tables to referencing
	parentName := s.Name
	fkColName := parentName + "_id"
	for _, tableName := range s.Tables {
		if tableName == parentName {
			continue
		}
		s.Fields = append(s.Fields, Field{
			Name:            fkColName,
			SourceType:      core.BSONTypeString,
			DestinationType: "VARCHAR",
			TableName:       tableName,
		})
	}
}

func (s *Table) FilterFields(table *Table) {
	out := &Table{
		Name:             table.Name,
		Database:         table.Database,
		WriteMode:        table.WriteMode,
		Fields:           []Field{},
		FieldMap:         map[string]Field{},
		Tables:           []string{},
		CreatedTimeField: table.CreatedTimeField,
		UpdatedTimeField: table.UpdatedTimeField,
		DeletedTimeField: table.DeletedTimeField,
	}

	for _, field := range table.Fields {
		f, ok := s.FieldMap[field.Name]
		if !ok {
			continue
		}
		f.DestinationType = field.DestinationType
		f.IsChildTable = field.IsChildTable
		f.IsPrimary = field.IsPrimary
		out.AddField(f)
	}

	*s = *out
}
