package sync_common

type databaseScheme struct {
	mongoURI string
	database string
	tables   []*Table
	tableMap map[string]*Table
}

type DatabaseScheme interface {
	Add(s *Table)
	Get(namespace, name string) (*Table, bool)
	Tables() []*Table
	MongoURI() string
	Database() string
}

func (c *databaseScheme) Add(s *Table) {
	if c.tableMap == nil {
		c.tableMap = make(map[string]*Table)
	}
	c.tables = append(c.tables, s)
	c.tableMap[tableKey(s.Database, s.Name)] = s
}

func (c *databaseScheme) Get(namespace, name string) (*Table, bool) {
	s, ok := c.tableMap[tableKey(namespace, name)]
	return s, ok
}

func (c *databaseScheme) Tables() []*Table {
	return c.tables
}

func (c *databaseScheme) MongoURI() string {
	return c.mongoURI
}

func (c *databaseScheme) Database() string {
	return c.database
}

func tableKey(database, name string) string {
	if database == "" {
		return name
	}
	return database + "." + name
}

func NewDatabaseScheme(mongoURI, database string) DatabaseScheme {
	return &databaseScheme{
		mongoURI: mongoURI,
		database: database,
		tableMap: make(map[string]*Table),
		tables:   []*Table{},
	}
}
