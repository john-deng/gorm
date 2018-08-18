package gorm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type GormDB interface {
	AddError(err error) error
	AddForeignKey(field string, dest string, onDelete string, onUpdate string) GormDB
	AddIndex(indexName string, columns ...string) GormDB
	AddUniqueIndex(indexName string, columns ...string) GormDB
	Assign(attrs ...interface{}) GormDB
	Association(column string) *Association
	Attrs(attrs ...interface{}) GormDB
	AutoMigrate(values ...interface{}) GormDB
	Begin() GormDB
	BlockGlobalUpdate(enable bool) GormDB
	Callback() *Callback
	Close() error
	Commit() GormDB
	CommonDB() SQLCommon
	Count(value interface{}) GormDB
	Create(value interface{}) GormDB
	CreateTable(models ...interface{}) GormDB
	SqlDB() *sql.DB
	Debug() GormDB
	Delete(value interface{}, where ...interface{}) GormDB
	Dialect() Dialect
	DropColumn(column string) GormDB
	DropTable(values ...interface{}) GormDB
	DropTableIfExists(values ...interface{}) GormDB
	Exec(sql string, values ...interface{}) GormDB
	Find(out interface{}, where ...interface{}) GormDB
	First(out interface{}, where ...interface{}) GormDB
	FirstOrCreate(out interface{}, where ...interface{}) GormDB
	FirstOrInit(out interface{}, where ...interface{}) GormDB
	Get(name string) (value interface{}, ok bool)
	GetErrors() []error
	Group(query string) GormDB
	HasBlockGlobalUpdate() bool
	HasTable(value interface{}) bool
	Having(query interface{}, values ...interface{}) GormDB
	InstantSet(name string, value interface{}) GormDB
	Joins(query string, args ...interface{}) GormDB
	Last(out interface{}, where ...interface{}) GormDB
	Limit(limit interface{}) GormDB
	LogMode(enable bool) GormDB
	Model(value interface{}) GormDB
	ModifyColumn(column string, typ string) GormDB
	New() GormDB
	NewRecord(value interface{}) bool
	NewScope(value interface{}) *Scope
	Not(query interface{}, args ...interface{}) GormDB
	Offset(offset interface{}) GormDB
	Omit(columns ...string) GormDB
	Or(query interface{}, args ...interface{}) GormDB
	Order(value interface{}, reorder ...bool) GormDB
	Pluck(column string, value interface{}) GormDB
	Preload(column string, conditions ...interface{}) GormDB
	QueryExpr() *Expression
	Raw(sql string, values ...interface{}) GormDB
	RecordNotFound() bool
	Related(value interface{}, foreignKeys ...string) GormDB
	RemoveForeignKey(field string, dest string) GormDB
	RemoveIndex(indexName string) GormDB
	Rollback() GormDB
	Row() *sql.Row
	Rows() (*sql.Rows, error)
	Save(value interface{}) GormDB
	Scan(dest interface{}) GormDB
	ScanRows(rows *sql.Rows, result interface{}) error
	Scopes(funcs ...func(GormDB) GormDB) GormDB
	Select(query interface{}, args ...interface{}) GormDB
	Set(name string, value interface{}) GormDB
	SetJoinTableHandler(source interface{}, column string, handler JoinTableHandlerInterface)
	SetLogger(log Logger) GormDB
	SingularTable(enable bool)
	SubQuery() *Expression
	Table(name string) GormDB
	Take(out interface{}, where ...interface{}) GormDB
	Unscoped() GormDB
	Update(attrs ...interface{}) GormDB
	UpdateColumn(attrs ...interface{}) GormDB
	UpdateColumns(values interface{}) GormDB
	Updates(values interface{}, ignoreProtectedAttrs ...bool) GormDB
	Where(query interface{}, args ...interface{}) GormDB
	Value() interface{}
	SetValue(v interface{}) GormDB
	Error() error
	SetError(err error) GormDB
	RowsAffected() int64
	SetRowsAffected(row int64) GormDB
	Search() *Search
	SetSearch(s *Search) GormDB
	Parent() GormDB
	SetParent(p GormDB) GormDB
	SQLCommonDB() SQLCommon
	SetSQLCommonDB(sc SQLCommon) GormDB
	Callbacks() *Callback
	SetCallbacks(cb *Callback) GormDB
	IsSingularTable() bool
	SetIsSingularTable(singularTable bool) GormDB
	SetDialect(d Dialect) GormDB
	Clone() GormDB
	Log(v ...interface{})
	Slog(sql string, t time.Time, vars ...interface{})
	Print(v ...interface{})
	Values() map[string]interface{}
	SetValues(vals map[string]interface{}) GormDB
}


// DB contains information for current db connection
type gormDB struct {
	value        interface{}
	err          error
	rowsAffected int64

	// single db
	db                SQLCommon
	blockGlobalUpdate bool
	logMode           int
	logger            Logger
	search            *Search
	values            map[string]interface{}

	// global db
	parent        GormDB
	callbacks     *Callback
	dialect       Dialect
	singularTable bool
}

// Open initialize a new db connection, need to import driver first, e.g:
//
//     import _ "github.com/go-sql-driver/mysql"
//     func main() {
//       db, err := gorm.Open("mysql", "user:password@/dbname?charset=utf8&parseTime=True&loc=Local")
//     }
// GORM has wrapped some drivers, for easier to remember driver's import path, so you could import the mysql driver with
//    import _ "gorm.io/gorm/dialects/mysql"
//    // import _ "gorm.io/gorm/dialects/postgres"
//    // import _ "gorm.io/gorm/dialects/sqlite"
//    // import _ "gorm.io/gorm/dialects/mssql"
func Open(dialect string, args ...interface{}) (db GormDB, err error) {
	if len(args) == 0 {
		err = errors.New("invalid database source")
		return nil, err
	}
	var source string
	var dbSQL SQLCommon
	var ownDbSQL bool

	switch value := args[0].(type) {
	case string:
		var driver = dialect
		if len(args) == 1 {
			source = value
		} else if len(args) >= 2 {
			driver = value
			source = args[1].(string)
		}
		dbSQL, err = sql.Open(driver, source)
		ownDbSQL = true
	case SQLCommon:
		dbSQL = value
		ownDbSQL = false
	default:
		return nil, fmt.Errorf("invalid database source: %v is not a valid type", value)
	}


	db = new(gormDB).
		SetSQLCommonDB(dbSQL).
		SetLogger(defaultLogger).
		SetValues(map[string]interface{}{}).
		SetCallbacks(DefaultCallback).
		SetDialect(newDialect(dialect, dbSQL))

	db.SetParent(db)

	if err != nil {
		return
	}
	// Send a ping to make sure the database connection is alive.
	if d, ok := dbSQL.(*sql.DB); ok {
		if err = d.Ping(); err != nil && ownDbSQL {
			d.Close()
		}
	}
	return
}

// New clone a new db connection without search conditions
func (s *gormDB) New() GormDB {
	clone := s.Clone()
	clone.SetSearch(nil)
	clone.SetValue(nil)
	return clone
}

type closer interface {
	Close() error
}

// Close close current db connection.  If database connection is not an io.Closer, returns an error.
func (s *gormDB) Close() error {
	if db, ok := s.Parent().SQLCommonDB().(closer); ok {
		return db.Close()
	}
	return errors.New("can't close current db")
}

// DB get `*sql.DB` from current connection
// If the underlying database connection is not a *sql.DB, returns nil
func (s *gormDB) SqlDB() *sql.DB {
	db, _ := s.db.(*sql.DB)
	return db
}

// CommonDB return the underlying `*sql.DB` or `*sql.Tx` instance, mainly intended to allow coexistence with legacy non-GORM code.
func (s *gormDB) CommonDB() SQLCommon {
	return s.db
}

// Dialect get dialect
func (s *gormDB) Dialect() Dialect {
	return s.dialect
}

// Callback return `Callbacks` container, you could add/change/delete callbacks with it
//     db.Callback().Create().Register("update_created_at", updateCreated)
// Refer https://jinzhu.github.io/gorm/development.html#callbacks
func (s *gormDB) Callback() *Callback {
	s.parent.SetCallbacks(s.parent.Callbacks().clone())
	return s.parent.Callbacks()
}

// SetLogger replace default logger
func (s *gormDB) SetLogger(log Logger) GormDB {
	s.logger = log
	return s
}

// LogMode set log mode, `true` for detailed logs, `false` for no log, default, will only print error logs
func (s *gormDB) LogMode(enable bool) GormDB {
	if enable {
		s.logMode = 2
	} else {
		s.logMode = 1
	}
	return s
}

// BlockGlobalUpdate if true, generates an error on update/delete without where clause.
// This is to prevent eventual error with empty objects updates/deletions
func (s *gormDB) BlockGlobalUpdate(enable bool) GormDB {
	s.blockGlobalUpdate = enable
	return s
}

// HasBlockGlobalUpdate return state of block
func (s *gormDB) HasBlockGlobalUpdate() bool {
	return s.blockGlobalUpdate
}

// SingularTable use singular table by default
func (s *gormDB) SingularTable(enable bool) {
	modelStructsMap = newModelStructsMap()
	s.parent.SetIsSingularTable(enable)
}

// NewScope create a scope for current operation
func (s *gormDB) NewScope(value interface{}) *Scope {
	dbClone := s.Clone()
	dbClone.SetValue(value)
	scope := &Scope{db: dbClone, Search: dbClone.Search().clone(), Value: value}
	return scope
}

// QueryExpr returns the query as expr object
func (s *gormDB) QueryExpr() *Expression {
	scope := s.NewScope(s.value)
	scope.InstanceSet("skip_bindvar", true)
	scope.prepareQuerySQL()

	return Expr(scope.SQL, scope.SQLVars...)
}

// SubQuery returns the query as sub query
func (s *gormDB) SubQuery() *Expression {
	scope := s.NewScope(s.value)
	scope.InstanceSet("skip_bindvar", true)
	scope.prepareQuerySQL()

	return Expr(fmt.Sprintf("(%v)", scope.SQL), scope.SQLVars...)
}

// Where return a new relation, filter records with given conditions, accepts `map`, `struct` or `string` as conditions, refer http://jinzhu.github.io/gorm/crud.html#query
func (s *gormDB) Where(query interface{}, args ...interface{}) GormDB {
	return s.Clone().Search().Where(query, args...).db
}

// Or filter records that match before conditions or this one, similar to `Where`
func (s *gormDB) Or(query interface{}, args ...interface{}) GormDB {
	return s.Clone().Search().Or(query, args...).db
}

// Not filter records that don't match current conditions, similar to `Where`
func (s *gormDB) Not(query interface{}, args ...interface{}) GormDB {
	return s.Clone().Search().Not(query, args...).db
}

// Limit specify the number of records to be retrieved
func (s *gormDB) Limit(limit interface{}) GormDB {
	return s.Clone().Search().Limit(limit).db
}

// Offset specify the number of records to skip before starting to return the records
func (s *gormDB) Offset(offset interface{}) GormDB {
	return s.Clone().Search().Offset(offset).db
}

// Order specify order when retrieve records from database, set reorder to `true` to overwrite defined conditions
//     db.Order("name DESC")
//     db.Order("name DESC", true) // reorder
//     db.Order(gorm.Expr("name = ? DESC", "first")) // sql expression
func (s *gormDB) Order(value interface{}, reorder ...bool) GormDB {
	return s.Clone().Search().Order(value, reorder...).db
}

// Select specify fields that you want to retrieve from database when querying, by default, will select all fields;
// When creating/updating, specify fields that you want to save to database
func (s *gormDB) Select(query interface{}, args ...interface{}) GormDB {
	return s.Clone().Search().Select(query, args...).db
}

// Omit specify fields that you want to ignore when saving to database for creating, updating
func (s *gormDB) Omit(columns ...string) GormDB {
	return s.Clone().Search().Omit(columns...).db
}

// Group specify the group method on the find
func (s *gormDB) Group(query string) GormDB {
	return s.Clone().Search().Group(query).db
}

// Having specify HAVING conditions for GROUP BY
func (s *gormDB) Having(query interface{}, values ...interface{}) GormDB {
	return s.Clone().Search().Having(query, values...).db
}

// Joins specify Joins conditions
//     db.Joins("JOIN emails ON emails.user_id = users.id AND emails.email = ?", "jinzhu@example.org").Find(&user)
func (s *gormDB) Joins(query string, args ...interface{}) GormDB {
	return s.Clone().Search().Joins(query, args...).db
}

// Scopes pass current database connection to arguments `func(Repository) Repository`, which could be used to add conditions dynamically
//     func AmountGreaterThan1000(db Repository) Repository {
//         return db.Where("amount > ?", 1000)
//     }
//
//     func OrderStatus(status []string) func (db Repository) Repository {
//         return func (db Repository) Repository {
//             return db.Scopes(AmountGreaterThan1000).Where("status in (?)", status)
//         }
//     }
//
//     db.Scopes(AmountGreaterThan1000, OrderStatus([]string{"paid", "shipped"})).Find(&orders)
// Refer https://jinzhu.github.io/gorm/crud.html#scopes
func (s *gormDB) Scopes(funcs ...func(GormDB) GormDB) GormDB {
	var db GormDB
	db = s
	for _, fn := range funcs {
		db = fn(db)
	}
	return db
}

// Unscoped return all record including deleted record, refer Soft Delete https://jinzhu.github.io/gorm/crud.html#soft-delete
func (s *gormDB) Unscoped() GormDB {
	return s.Clone().Search().unscoped().db
}

// Attrs initialize struct with argument if record not found with `FirstOrInit` https://jinzhu.github.io/gorm/crud.html#firstorinit or `FirstOrCreate` https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (s *gormDB) Attrs(attrs ...interface{}) GormDB {
	return s.Clone().Search().Attrs(attrs...).db
}

// Assign assign result with argument regardless it is found or not with `FirstOrInit` https://jinzhu.github.io/gorm/crud.html#firstorinit or `FirstOrCreate` https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (s *gormDB) Assign(attrs ...interface{}) GormDB {
	return s.Clone().Search().Assign(attrs...).db
}

// First find first record that match given conditions, order by primary key
func (s *gormDB) First(out interface{}, where ...interface{}) GormDB {
	newScope := s.NewScope(out)
	newScope.Search.Limit(1)
	return newScope.Set("gorm:order_by_primary_key", "ASC").
		inlineCondition(where...).callCallbacks(s.parent.Callbacks().queries).db
}

// Take return a record that match given conditions, the order will depend on the database implementation
func (s *gormDB) Take(out interface{}, where ...interface{}) GormDB {
	newScope := s.NewScope(out)
	newScope.Search.Limit(1)
	return newScope.inlineCondition(where...).callCallbacks(s.parent.Callbacks().queries).db
}

// Last find last record that match given conditions, order by primary key
func (s *gormDB) Last(out interface{}, where ...interface{}) GormDB {
	newScope := s.NewScope(out)
	newScope.Search.Limit(1)
	return newScope.Set("gorm:order_by_primary_key", "DESC").
		inlineCondition(where...).callCallbacks(s.parent.Callbacks().queries).db
}

// Find find records that match given conditions
func (s *gormDB) Find(out interface{}, where ...interface{}) GormDB {
	return s.NewScope(out).inlineCondition(where...).callCallbacks(s.parent.Callbacks().queries).db
}

// Scan scan value to a struct
func (s *gormDB) Scan(dest interface{}) GormDB {
	return s.NewScope(s.value).Set("gorm:query_destination", dest).callCallbacks(s.parent.Callbacks().queries).db
}

// Row return `*sql.Row` with given conditions
func (s *gormDB) Row() *sql.Row {
	return s.NewScope(s.value).row()
}

// Rows return `*sql.Rows` with given conditions
func (s *gormDB) Rows() (*sql.Rows, error) {
	return s.NewScope(s.value).rows()
}

// ScanRows scan `*sql.Rows` to give struct
func (s *gormDB) ScanRows(rows *sql.Rows, result interface{}) error {
	var (
		scope        = s.NewScope(result)
		clone        = scope.db
		columns, err = rows.Columns()
	)

	if clone.AddError(err) == nil {
		scope.scan(rows, columns, scope.Fields())
	}

	return clone.Error()
}

// Pluck used to query single column from a model as a map
//     var ages []int64
//     db.Find(&users).Pluck("age", &ages)
func (s *gormDB) Pluck(column string, value interface{}) GormDB {
	return s.NewScope(s.value).pluck(column, value).db
}

// Count get how many records for a model
func (s *gormDB) Count(value interface{}) GormDB {
	return s.NewScope(s.value).count(value).db
}

// Related get related associations
func (s *gormDB) Related(value interface{}, foreignKeys ...string) GormDB {
	return s.NewScope(s.value).related(value, foreignKeys...).db
}

// FirstOrInit find first matched record or initialize a new one with given conditions (only works with struct, map conditions)
// https://jinzhu.github.io/gorm/crud.html#firstorinit
func (s *gormDB) FirstOrInit(out interface{}, where ...interface{}) GormDB {
	c := s.Clone()
	if result := c.First(out, where...); result.Error() != nil {
		if !result.RecordNotFound() {
			return result
		}
		c.NewScope(out).inlineCondition(where...).initialize()
	} else {
		c.NewScope(out).updatedAttrsWithValues(c.Search().assignAttrs)
	}
	return c
}

// FirstOrCreate find first matched record or create a new one with given conditions (only works with struct, map conditions)
// https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (s *gormDB) FirstOrCreate(out interface{}, where ...interface{}) GormDB {
	c := s.Clone()
	if result := s.First(out, where...); result.Error() != nil {
		if !result.RecordNotFound() {
			return result
		}
		return c.NewScope(out).inlineCondition(where...).initialize().callCallbacks(c.Parent().Callbacks().creates).db
	} else if len(c.Search().assignAttrs) > 0 {
		return c.NewScope(out).InstanceSet("gorm:update_interface", c.Search().assignAttrs).callCallbacks(c.Parent().Callbacks().updates).db
	}
	return c
}

// Update update attributes with callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (s *gormDB) Update(attrs ...interface{}) GormDB {
	return s.Updates(toSearchableMap(attrs...), true)
}

// Updates update attributes with callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (s *gormDB) Updates(values interface{}, ignoreProtectedAttrs ...bool) GormDB {
	return s.NewScope(s.value).
		Set("gorm:ignore_protected_attrs", len(ignoreProtectedAttrs) > 0).
		InstanceSet("gorm:update_interface", values).
		callCallbacks(s.parent.Callbacks().updates).db
}

// UpdateColumn update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (s *gormDB) UpdateColumn(attrs ...interface{}) GormDB {
	return s.UpdateColumns(toSearchableMap(attrs...))
}

// UpdateColumns update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (s *gormDB) UpdateColumns(values interface{}) GormDB {
	return s.NewScope(s.value).
		Set("gorm:update_column", true).
		Set("gorm:save_associations", false).
		InstanceSet("gorm:update_interface", values).
		callCallbacks(s.parent.Callbacks().updates).db
}

// Save update value in database, if the value doesn't have primary key, will insert it
func (s *gormDB) Save(value interface{}) GormDB {
	scope := s.NewScope(value)
	if !scope.PrimaryKeyZero() {
		newDB := scope.callCallbacks(s.parent.Callbacks().updates).db
		if newDB.Error() == nil && newDB.RowsAffected() == 0 {
			return s.New().FirstOrCreate(value)
		}
		return newDB
	}
	return scope.callCallbacks(s.Parent().Callbacks().creates).db
}

// Create insert the value into database
func (s *gormDB) Create(value interface{}) GormDB {
	scope := s.NewScope(value)
	return scope.callCallbacks(s.parent.Callbacks().creates).db
}

// Delete delete value match given conditions, if the value has primary key, then will including the primary key as condition
func (s *gormDB) Delete(value interface{}, where ...interface{}) GormDB {
	return s.NewScope(value).inlineCondition(where...).callCallbacks(s.parent.Callbacks().deletes).db
}

// Raw use raw sql as conditions, won't run it unless invoked by other methods
//    db.Raw("SELECT name, age FROM users WHERE name = ?", 3).Scan(&result)
func (s *gormDB) Raw(sql string, values ...interface{}) GormDB {
	return s.Clone().Search().Raw(true).Where(sql, values...).db
}

// Exec execute raw sql
func (s *gormDB) Exec(sql string, values ...interface{}) GormDB {
	scope := s.NewScope(nil)
	generatedSQL := scope.buildCondition(map[string]interface{}{"query": sql, "args": values}, true)
	generatedSQL = strings.TrimSuffix(strings.TrimPrefix(generatedSQL, "("), ")")
	scope.Raw(generatedSQL)
	return scope.Exec().db
}

// Model specify the model you would like to run db operations
//    // update all users's name to `hello`
//    db.Model(&User{}).Update("name", "hello")
//    // if user's primary key is non-blank, will use it as condition, then will only update the user's name to `hello`
//    db.Model(&user).Update("name", "hello")
func (s *gormDB) Model(value interface{}) GormDB {
	c := s.Clone()
	c.SetValue(value)
	return c
}

// Table specify the table you would like to run db operations
func (s *gormDB) Table(name string) GormDB {
	clone := s.Clone()
	clone.Search().Table(name)
	clone.SetValue(nil)
	return clone
}

// Debug start debug mode
func (s *gormDB) Debug() GormDB {
	return s.Clone().LogMode(true)
}

// Begin begin a transaction
func (s *gormDB) Begin() GormDB {
	c := s.Clone()
	if db, ok := c.SQLCommonDB().(sqlDb); ok && db != nil {
		tx, err := db.Begin()
		c.SetSQLCommonDB(interface{}(tx).(SQLCommon))

		c.Dialect().SetDB(c.SQLCommonDB())
		c.AddError(err)
	} else {
		c.AddError(ErrCantStartTransaction)
	}
	return c
}

// Commit commit a transaction
func (s *gormDB) Commit() GormDB {
	var emptySQLTx *sql.Tx
	if db, ok := s.db.(sqlTx); ok && db != nil && db != emptySQLTx {
		s.AddError(db.Commit())
	} else {
		s.AddError(ErrInvalidTransaction)
	}
	return s
}

// Rollback rollback a transaction
func (s *gormDB) Rollback() GormDB {
	var emptySQLTx *sql.Tx
	if db, ok := s.db.(sqlTx); ok && db != nil && db != emptySQLTx {
		s.AddError(db.Rollback())
	} else {
		s.AddError(ErrInvalidTransaction)
	}
	return s
}

// NewRecord check if value's primary key is blank
func (s *gormDB) NewRecord(value interface{}) bool {
	return s.NewScope(value).PrimaryKeyZero()
}

// RecordNotFound check if returning ErrRecordNotFound error
func (s *gormDB) RecordNotFound() bool {
	for _, err := range s.GetErrors() {
		if err == ErrRecordNotFound {
			return true
		}
	}
	return false
}

// CreateTable create table for models
func (s *gormDB) CreateTable(models ...interface{}) GormDB {
	db := s.Unscoped()
	for _, model := range models {
		db = db.NewScope(model).createTable().db
	}
	return db
}

// DropTable drop table for models
func (s *gormDB) DropTable(values ...interface{}) GormDB {
	db := s.Clone()
	for _, value := range values {
		if tableName, ok := value.(string); ok {
			db = db.Table(tableName)
		}

		db = db.NewScope(value).dropTable().db
	}
	return db
}

// DropTableIfExists drop table if it is exist
func (s *gormDB) DropTableIfExists(values ...interface{}) GormDB {
	db := s.Clone()
	for _, value := range values {
		if s.HasTable(value) {
			db.AddError(s.DropTable(value).Error())
		}
	}
	return db
}

// HasTable check has table or not
func (s *gormDB) HasTable(value interface{}) bool {
	var (
		scope     = s.NewScope(value)
		tableName string
	)

	if name, ok := value.(string); ok {
		tableName = name
	} else {
		tableName = scope.TableName()
	}

	has := scope.Dialect().HasTable(tableName)
	s.AddError(scope.db.Error())
	return has
}

// AutoMigrate run auto migration for given models, will only add missing fields, won't delete/change current data
func (s *gormDB) AutoMigrate(values ...interface{}) GormDB {
	db := s.Unscoped()
	for _, value := range values {
		db = db.NewScope(value).autoMigrate().db
	}
	return db
}

// ModifyColumn modify column to type
func (s *gormDB) ModifyColumn(column string, typ string) GormDB {
	scope := s.NewScope(s.value)
	scope.modifyColumn(column, typ)
	return scope.db
}

// DropColumn drop a column
func (s *gormDB) DropColumn(column string) GormDB {
	scope := s.NewScope(s.value)
	scope.dropColumn(column)
	return scope.db
}

// AddIndex add index for columns with given name
func (s *gormDB) AddIndex(indexName string, columns ...string) GormDB {
	scope := s.Unscoped().NewScope(s.value)
	scope.addIndex(false, indexName, columns...)
	return scope.db
}

// AddUniqueIndex add unique index for columns with given name
func (s *gormDB) AddUniqueIndex(indexName string, columns ...string) GormDB {
	scope := s.Unscoped().NewScope(s.value)
	scope.addIndex(true, indexName, columns...)
	return scope.db
}

// RemoveIndex remove index with name
func (s *gormDB) RemoveIndex(indexName string) GormDB {
	scope := s.NewScope(s.value)
	scope.removeIndex(indexName)
	return scope.db
}

// AddForeignKey Add foreign key to the given scope, e.g:
//     db.Model(&User{}).AddForeignKey("city_id", "cities(id)", "RESTRICT", "RESTRICT")
func (s *gormDB) AddForeignKey(field string, dest string, onDelete string, onUpdate string) GormDB {
	scope := s.NewScope(s.value)
	scope.addForeignKey(field, dest, onDelete, onUpdate)
	return scope.db
}

// RemoveForeignKey Remove foreign key from the given scope, e.g:
//     db.Model(&User{}).RemoveForeignKey("city_id", "cities(id)")
func (s *gormDB) RemoveForeignKey(field string, dest string) GormDB {
	scope := s.Clone().NewScope(s.value)
	scope.removeForeignKey(field, dest)
	return scope.db
}

// Association start `Association Mode` to handler relations things easir in that mode, refer: https://jinzhu.github.io/gorm/associations.html#association-mode
func (s *gormDB) Association(column string) *Association {
	var err error
	var scope = s.Set("gorm:association:source", s.value).NewScope(s.value)

	if primaryField := scope.PrimaryField(); primaryField.IsBlank {
		err = errors.New("primary key can't be nil")
	} else {
		if field, ok := scope.FieldByName(column); ok {
			if field.Relationship == nil || len(field.Relationship.ForeignFieldNames) == 0 {
				err = fmt.Errorf("invalid association %v for %v", column, scope.IndirectValue().Type())
			} else {
				return &Association{scope: scope, column: column, field: field}
			}
		} else {
			err = fmt.Errorf("%v doesn't have column %v", scope.IndirectValue().Type(), column)
		}
	}

	return &Association{err: err}
}

// Preload preload associations with given conditions
//    db.Preload("Orders", "state NOT IN (?)", "cancelled").Find(&users)
func (s *gormDB) Preload(column string, conditions ...interface{}) GormDB {
	return s.Clone().Search().Preload(column, conditions...).db
}

// Set set setting by name, which could be used in callbacks, will clone a new db, and update its setting
func (s *gormDB) Set(name string, value interface{}) GormDB {
	return s.Clone().InstantSet(name, value)
}

// InstantSet instant set setting, will affect current db
func (s *gormDB) InstantSet(name string, value interface{}) GormDB {
	s.values[name] = value
	return s
}

// Get get setting by name
func (s *gormDB) Get(name string) (value interface{}, ok bool) {
	value, ok = s.values[name]
	return
}

// SetJoinTableHandler set a model's join table handler for a relation
func (s *gormDB) SetJoinTableHandler(source interface{}, column string, handler JoinTableHandlerInterface) {
	scope := s.NewScope(source)
	for _, field := range scope.GetModelStruct().StructFields {
		if field.Name == column || field.DBName == column {
			if many2many := field.TagSettings["MANY2MANY"]; many2many != "" {
				source := (&Scope{Value: source}).GetModelStruct().ModelType
				destination := (&Scope{Value: reflect.New(field.Struct.Type).Interface()}).GetModelStruct().ModelType
				handler.Setup(field.Relationship, many2many, source, destination)
				field.Relationship.JoinTableHandler = handler
				if table := handler.Table(s); scope.Dialect().HasTable(table) {
					s.Table(table).AutoMigrate(handler)
				}
			}
		}
	}
}

// AddError add error to the db
func (s *gormDB) AddError(err error) error {
	if err != nil {
		if err != ErrRecordNotFound {
			if s.logMode == 0 {
				go s.Print(fileWithLineNum(), err)
			} else {
				s.Log(err)
			}

			errors := Errors(s.GetErrors())
			errors = errors.Add(err)
			if len(errors) > 1 {
				err = errors
			}
		}

		s.SetError(err)
	}
	return err
}

// GetErrors get happened errors from the db
func (s *gormDB) GetErrors() []error {
	if errs, ok := s.Error().(Errors); ok {
		return errs
	} else if s.Error() != nil {
		return []error{s.Error()}
	}
	return []error{}
}

func (s *gormDB) Value() interface{} {
	return s.value
}

func (s *gormDB) SetValue(v interface{}) GormDB {
	s.value = v
	return s
}


func (s *gormDB) Error() error {
	return s.err
}

func (s *gormDB) SetError(err error) GormDB {
	s.err = err
	return s
}

func (s *gormDB) RowsAffected() int64 {
	return s.rowsAffected
}

func (s *gormDB) SetRowsAffected(row int64) GormDB {
	s.rowsAffected = row
	return s
}

func (s *gormDB) Search() *Search {
	return s.search
}

func (s *gormDB) SetSearch(search *Search) GormDB {
	s.search = search
	return s
}

func (s *gormDB) Parent() GormDB {
	return s.parent
}

func (s *gormDB) SetParent(p GormDB) GormDB {
	s.parent = p
	return s
}

func (s *gormDB) SQLCommonDB() SQLCommon {
	return s.db
}

func (s *gormDB) SetSQLCommonDB(sc SQLCommon) GormDB {
	s.db = sc
	return s
}

func (s *gormDB) Callbacks() *Callback {
	return s.callbacks
}

func (s *gormDB) SetCallbacks(cb *Callback) GormDB {
	s.callbacks = cb
	return s
}

func (s *gormDB) IsSingularTable() bool {
	return s.singularTable
}

func (s *gormDB) SetIsSingularTable(singularTable bool) GormDB {
	s.singularTable = singularTable
	return s
}
func (s *gormDB) Values() map[string]interface{} {
	return s.values
}

func (s *gormDB) SetValues(vals map[string]interface{}) GormDB {
	s.values = vals
	return s
}

func (s *gormDB) SetDialect(d Dialect) GormDB {
	s.dialect = d
	return s
}


////////////////////////////////////////////////////////////////////////////////
// Private Methods For DB
////////////////////////////////////////////////////////////////////////////////

func (s *gormDB) Clone() GormDB {
	db := &gormDB{
		db:                s.db,
		parent:            s.parent,
		logger:            s.logger,
		logMode:           s.logMode,
		values:            map[string]interface{}{},
		value:             s.value,
		err:               s.Error(),
		blockGlobalUpdate: s.blockGlobalUpdate,
		dialect:           newDialect(s.dialect.GetName(), s.db),
	}

	for key, value := range s.values {
		db.values[key] = value
	}

	if s.search == nil {
		db.search = &Search{limit: -1, offset: -1}
	} else {
		db.search = s.Search().clone()
	}

	db.Search().db = db
	return db
}

func (s *gormDB) Print(v ...interface{}) {
	s.logger.Print(v...)
}

func (s *gormDB) Log(v ...interface{}) {
	if s != nil && s.logMode == 2 {
		s.Print(append([]interface{}{"log", fileWithLineNum()}, v...)...)
	}
}

func (s *gormDB) Slog(sql string, t time.Time, vars ...interface{}) {
	if s.logMode == 2 {
		s.Print("sql", fileWithLineNum(), NowFunc().Sub(t), sql, vars, s.RowsAffected())
	}
}
