package gorm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Repository interface {
	AddError(err error) error
	AddForeignKey(field string, dest string, onDelete string, onUpdate string) Repository
	AddIndex(indexName string, columns ...string) Repository
	AddUniqueIndex(indexName string, columns ...string) Repository
	Assign(attrs ...interface{}) Repository
	Association(column string) *Association
	Attrs(attrs ...interface{}) Repository
	AutoMigrate(values ...interface{}) Repository
	Begin() Repository
	BlockGlobalUpdate(enable bool) Repository
	Callback() *Callback
	Close() error
	Commit() Repository
	CommonDB() SQLCommon
	Count(value interface{}) Repository
	Create(value interface{}) Repository
	CreateTable(models ...interface{}) Repository
	SqlDB() *sql.DB
	Debug() Repository
	Delete(value interface{}, where ...interface{}) Repository
	Dialect() Dialect
	DropColumn(column string) Repository
	DropTable(values ...interface{}) Repository
	DropTableIfExists(values ...interface{}) Repository
	Exec(sql string, values ...interface{}) Repository
	Find(out interface{}, where ...interface{}) Repository
	First(out interface{}, where ...interface{}) Repository
	FirstOrCreate(out interface{}, where ...interface{}) Repository
	FirstOrInit(out interface{}, where ...interface{}) Repository
	Get(name string) (value interface{}, ok bool)
	GetErrors() []error
	Group(query string) Repository
	HasBlockGlobalUpdate() bool
	HasTable(value interface{}) bool
	Having(query interface{}, values ...interface{}) Repository
	InstantSet(name string, value interface{}) Repository
	Joins(query string, args ...interface{}) Repository
	Last(out interface{}, where ...interface{}) Repository
	Limit(limit interface{}) Repository
	LogMode(enable bool) Repository
	Model(value interface{}) Repository
	ModifyColumn(column string, typ string) Repository
	New() Repository
	NewRecord(value interface{}) bool
	NewScope(value interface{}) *Scope
	Not(query interface{}, args ...interface{}) Repository
	Offset(offset interface{}) Repository
	Omit(columns ...string) Repository
	Or(query interface{}, args ...interface{}) Repository
	Order(value interface{}, reorder ...bool) Repository
	Pluck(column string, value interface{}) Repository
	Preload(column string, conditions ...interface{}) Repository
	QueryExpr() *Expression
	Raw(sql string, values ...interface{}) Repository
	RecordNotFound() bool
	Related(value interface{}, foreignKeys ...string) Repository
	RemoveForeignKey(field string, dest string) Repository
	RemoveIndex(indexName string) Repository
	Rollback() Repository
	Row() *sql.Row
	Rows() (*sql.Rows, error)
	Save(value interface{}) Repository
	Scan(dest interface{}) Repository
	ScanRows(rows *sql.Rows, result interface{}) error
	Scopes(funcs ...func(Repository) Repository) Repository
	Select(query interface{}, args ...interface{}) Repository
	Set(name string, value interface{}) Repository
	SetJoinTableHandler(source interface{}, column string, handler JoinTableHandlerInterface)
	SetLogger(log Logger) Repository
	SingularTable(enable bool)
	SubQuery() *Expression
	Table(name string) Repository
	Take(out interface{}, where ...interface{}) Repository
	Unscoped() Repository
	Update(attrs ...interface{}) Repository
	UpdateColumn(attrs ...interface{}) Repository
	UpdateColumns(values interface{}) Repository
	Updates(values interface{}, ignoreProtectedAttrs ...bool) Repository
	Where(query interface{}, args ...interface{}) Repository
	Value() interface{}
	SetValue(v interface{}) Repository
	Error() error
	SetError(err error) Repository
	RowsAffected() int64
	SetRowsAffected(row int64) Repository
	Search() *Search
	SetSearch(s *Search) Repository
	Parent() Repository
	SetParent(p Repository) Repository
	SQLCommonDB() SQLCommon
	SetSQLCommonDB(sc SQLCommon) Repository
	Callbacks() *Callback
	SetCallbacks(cb *Callback) Repository
	IsSingularTable() bool
	SetIsSingularTable(singularTable bool) Repository
	SetDialect(d Dialect) Repository
	Clone() Repository
	Log(v ...interface{})
	Slog(sql string, t time.Time, vars ...interface{})
	Print(v ...interface{})
	Values() map[string]interface{}
	SetValues(vals map[string]interface{}) Repository
}


// DB contains information for current db connection
type repository struct {
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
	parent        Repository
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
func Open(dialect string, args ...interface{}) (db Repository, err error) {
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


	db = new(repository).
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
func (r *repository) New() Repository {
	clone := r.Clone()
	clone.SetSearch(nil)
	clone.SetValue(nil)
	return clone
}

type closer interface {
	Close() error
}

// Close close current db connection.  If database connection is not an io.Closer, returns an error.
func (r *repository) Close() error {
	if db, ok := r.Parent().SQLCommonDB().(closer); ok {
		return db.Close()
	}
	return errors.New("can't close current db")
}

// DB get `*sql.DB` from current connection
// If the underlying database connection is not a *sql.DB, returns nil
func (r *repository) SqlDB() *sql.DB {
	db, _ := r.db.(*sql.DB)
	return db
}

// CommonDB return the underlying `*sql.DB` or `*sql.Tx` instance, mainly intended to allow coexistence with legacy non-GORM code.
func (r *repository) CommonDB() SQLCommon {
	return r.db
}

// Dialect get dialect
func (r *repository) Dialect() Dialect {
	return r.dialect
}

// Callback return `Callbacks` container, you could add/change/delete callbacks with it
//     db.Callback().Create().Register("update_created_at", updateCreated)
// Refer https://jinzhu.github.io/gorm/development.html#callbacks
func (r *repository) Callback() *Callback {
	r.parent.SetCallbacks(r.parent.Callbacks().clone())
	return r.parent.Callbacks()
}

// SetLogger replace default logger
func (r *repository) SetLogger(log Logger) Repository {
	r.logger = log
	return r
}

// LogMode set log mode, `true` for detailed logs, `false` for no log, default, will only print error logs
func (r *repository) LogMode(enable bool) Repository {
	if enable {
		r.logMode = 2
	} else {
		r.logMode = 1
	}
	return r
}

// BlockGlobalUpdate if true, generates an error on update/delete without where clause.
// This is to prevent eventual error with empty objects updates/deletions
func (r *repository) BlockGlobalUpdate(enable bool) Repository {
	r.blockGlobalUpdate = enable
	return r
}

// HasBlockGlobalUpdate return state of block
func (r *repository) HasBlockGlobalUpdate() bool {
	return r.blockGlobalUpdate
}

// SingularTable use singular table by default
func (r *repository) SingularTable(enable bool) {
	modelStructsMap = newModelStructsMap()
	r.parent.SetIsSingularTable(enable)
}

// NewScope create a scope for current operation
func (r *repository) NewScope(value interface{}) *Scope {
	dbClone := r.Clone()
	dbClone.SetValue(value)
	scope := &Scope{db: dbClone, Search: dbClone.Search().clone(), Value: value}
	return scope
}

// QueryExpr returns the query as expr object
func (r *repository) QueryExpr() *Expression {
	scope := r.NewScope(r.value)
	scope.InstanceSet("skip_bindvar", true)
	scope.prepareQuerySQL()

	return Expr(scope.SQL, scope.SQLVars...)
}

// SubQuery returns the query as sub query
func (r *repository) SubQuery() *Expression {
	scope := r.NewScope(r.value)
	scope.InstanceSet("skip_bindvar", true)
	scope.prepareQuerySQL()

	return Expr(fmt.Sprintf("(%v)", scope.SQL), scope.SQLVars...)
}

// Where return a new relation, filter records with given conditions, accepts `map`, `struct` or `string` as conditions, refer http://jinzhu.github.io/gorm/crud.html#query
func (r *repository) Where(query interface{}, args ...interface{}) Repository {
	return r.Clone().Search().Where(query, args...).db
}

// Or filter records that match before conditions or this one, similar to `Where`
func (r *repository) Or(query interface{}, args ...interface{}) Repository {
	return r.Clone().Search().Or(query, args...).db
}

// Not filter records that don't match current conditions, similar to `Where`
func (r *repository) Not(query interface{}, args ...interface{}) Repository {
	return r.Clone().Search().Not(query, args...).db
}

// Limit specify the number of records to be retrieved
func (r *repository) Limit(limit interface{}) Repository {
	return r.Clone().Search().Limit(limit).db
}

// Offset specify the number of records to skip before starting to return the records
func (r *repository) Offset(offset interface{}) Repository {
	return r.Clone().Search().Offset(offset).db
}

// Order specify order when retrieve records from database, set reorder to `true` to overwrite defined conditions
//     db.Order("name DESC")
//     db.Order("name DESC", true) // reorder
//     db.Order(gorm.Expr("name = ? DESC", "first")) // sql expression
func (r *repository) Order(value interface{}, reorder ...bool) Repository {
	return r.Clone().Search().Order(value, reorder...).db
}

// Select specify fields that you want to retrieve from database when querying, by default, will select all fields;
// When creating/updating, specify fields that you want to save to database
func (r *repository) Select(query interface{}, args ...interface{}) Repository {
	return r.Clone().Search().Select(query, args...).db
}

// Omit specify fields that you want to ignore when saving to database for creating, updating
func (r *repository) Omit(columns ...string) Repository {
	return r.Clone().Search().Omit(columns...).db
}

// Group specify the group method on the find
func (r *repository) Group(query string) Repository {
	return r.Clone().Search().Group(query).db
}

// Having specify HAVING conditions for GROUP BY
func (r *repository) Having(query interface{}, values ...interface{}) Repository {
	return r.Clone().Search().Having(query, values...).db
}

// Joins specify Joins conditions
//     db.Joins("JOIN emails ON emails.user_id = users.id AND emails.email = ?", "jinzhu@example.org").Find(&user)
func (r *repository) Joins(query string, args ...interface{}) Repository {
	return r.Clone().Search().Joins(query, args...).db
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
func (r *repository) Scopes(funcs ...func(Repository) Repository) Repository {
	var db Repository
	db = r
	for _, fn := range funcs {
		db = fn(db)
	}
	return db
}

// Unscoped return all record including deleted record, refer Soft Delete https://jinzhu.github.io/gorm/crud.html#soft-delete
func (r *repository) Unscoped() Repository {
	return r.Clone().Search().unscoped().db
}

// Attrs initialize struct with argument if record not found with `FirstOrInit` https://jinzhu.github.io/gorm/crud.html#firstorinit or `FirstOrCreate` https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (r *repository) Attrs(attrs ...interface{}) Repository {
	return r.Clone().Search().Attrs(attrs...).db
}

// Assign assign result with argument regardless it is found or not with `FirstOrInit` https://jinzhu.github.io/gorm/crud.html#firstorinit or `FirstOrCreate` https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (r *repository) Assign(attrs ...interface{}) Repository {
	return r.Clone().Search().Assign(attrs...).db
}

// First find first record that match given conditions, order by primary key
func (r *repository) First(out interface{}, where ...interface{}) Repository {
	newScope := r.NewScope(out)
	newScope.Search.Limit(1)
	return newScope.Set("gorm:order_by_primary_key", "ASC").
		inlineCondition(where...).callCallbacks(r.parent.Callbacks().queries).db
}

// Take return a record that match given conditions, the order will depend on the database implementation
func (r *repository) Take(out interface{}, where ...interface{}) Repository {
	newScope := r.NewScope(out)
	newScope.Search.Limit(1)
	return newScope.inlineCondition(where...).callCallbacks(r.parent.Callbacks().queries).db
}

// Last find last record that match given conditions, order by primary key
func (r *repository) Last(out interface{}, where ...interface{}) Repository {
	newScope := r.NewScope(out)
	newScope.Search.Limit(1)
	return newScope.Set("gorm:order_by_primary_key", "DESC").
		inlineCondition(where...).callCallbacks(r.parent.Callbacks().queries).db
}

// Find find records that match given conditions
func (r *repository) Find(out interface{}, where ...interface{}) Repository {
	return r.NewScope(out).inlineCondition(where...).callCallbacks(r.parent.Callbacks().queries).db
}

// Scan scan value to a struct
func (r *repository) Scan(dest interface{}) Repository {
	return r.NewScope(r.value).Set("gorm:query_destination", dest).callCallbacks(r.parent.Callbacks().queries).db
}

// Row return `*sql.Row` with given conditions
func (r *repository) Row() *sql.Row {
	return r.NewScope(r.value).row()
}

// Rows return `*sql.Rows` with given conditions
func (r *repository) Rows() (*sql.Rows, error) {
	return r.NewScope(r.value).rows()
}

// ScanRows scan `*sql.Rows` to give struct
func (r *repository) ScanRows(rows *sql.Rows, result interface{}) error {
	var (
		scope        = r.NewScope(result)
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
func (r *repository) Pluck(column string, value interface{}) Repository {
	return r.NewScope(r.value).pluck(column, value).db
}

// Count get how many records for a model
func (r *repository) Count(value interface{}) Repository {
	return r.NewScope(r.value).count(value).db
}

// Related get related associations
func (r *repository) Related(value interface{}, foreignKeys ...string) Repository {
	return r.NewScope(r.value).related(value, foreignKeys...).db
}

// FirstOrInit find first matched record or initialize a new one with given conditions (only works with struct, map conditions)
// https://jinzhu.github.io/gorm/crud.html#firstorinit
func (r *repository) FirstOrInit(out interface{}, where ...interface{}) Repository {
	c := r.Clone()
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
func (r *repository) FirstOrCreate(out interface{}, where ...interface{}) Repository {
	c := r.Clone()
	if result := r.First(out, where...); result.Error() != nil {
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
func (r *repository) Update(attrs ...interface{}) Repository {
	return r.Updates(toSearchableMap(attrs...), true)
}

// Updates update attributes with callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (r *repository) Updates(values interface{}, ignoreProtectedAttrs ...bool) Repository {
	return r.NewScope(r.value).
		Set("gorm:ignore_protected_attrs", len(ignoreProtectedAttrs) > 0).
		InstanceSet("gorm:update_interface", values).
		callCallbacks(r.parent.Callbacks().updates).db
}

// UpdateColumn update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (r *repository) UpdateColumn(attrs ...interface{}) Repository {
	return r.UpdateColumns(toSearchableMap(attrs...))
}

// UpdateColumns update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (r *repository) UpdateColumns(values interface{}) Repository {
	return r.NewScope(r.value).
		Set("gorm:update_column", true).
		Set("gorm:save_associations", false).
		InstanceSet("gorm:update_interface", values).
		callCallbacks(r.parent.Callbacks().updates).db
}

// Save update value in database, if the value doesn't have primary key, will insert it
func (r *repository) Save(value interface{}) Repository {
	scope := r.NewScope(value)
	if !scope.PrimaryKeyZero() {
		newDB := scope.callCallbacks(r.parent.Callbacks().updates).db
		if newDB.Error() == nil && newDB.RowsAffected() == 0 {
			return r.New().FirstOrCreate(value)
		}
		return newDB
	}
	return scope.callCallbacks(r.Parent().Callbacks().creates).db
}

// Create insert the value into database
func (r *repository) Create(value interface{}) Repository {
	scope := r.NewScope(value)
	return scope.callCallbacks(r.parent.Callbacks().creates).db
}

// Delete delete value match given conditions, if the value has primary key, then will including the primary key as condition
func (r *repository) Delete(value interface{}, where ...interface{}) Repository {
	return r.NewScope(value).inlineCondition(where...).callCallbacks(r.parent.Callbacks().deletes).db
}

// Raw use raw sql as conditions, won't run it unless invoked by other methods
//    db.Raw("SELECT name, age FROM users WHERE name = ?", 3).Scan(&result)
func (r *repository) Raw(sql string, values ...interface{}) Repository {
	return r.Clone().Search().Raw(true).Where(sql, values...).db
}

// Exec execute raw sql
func (r *repository) Exec(sql string, values ...interface{}) Repository {
	scope := r.NewScope(nil)
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
func (r *repository) Model(value interface{}) Repository {
	c := r.Clone()
	c.SetValue(value)
	return c
}

// Table specify the table you would like to run db operations
func (r *repository) Table(name string) Repository {
	clone := r.Clone()
	clone.Search().Table(name)
	clone.SetValue(nil)
	return clone
}

// Debug start debug mode
func (r *repository) Debug() Repository {
	return r.Clone().LogMode(true)
}

// Begin begin a transaction
func (r *repository) Begin() Repository {
	c := r.Clone()
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
func (r *repository) Commit() Repository {
	var emptySQLTx *sql.Tx
	if db, ok := r.db.(sqlTx); ok && db != nil && db != emptySQLTx {
		r.AddError(db.Commit())
	} else {
		r.AddError(ErrInvalidTransaction)
	}
	return r
}

// Rollback rollback a transaction
func (r *repository) Rollback() Repository {
	var emptySQLTx *sql.Tx
	if db, ok := r.db.(sqlTx); ok && db != nil && db != emptySQLTx {
		r.AddError(db.Rollback())
	} else {
		r.AddError(ErrInvalidTransaction)
	}
	return r
}

// NewRecord check if value's primary key is blank
func (r *repository) NewRecord(value interface{}) bool {
	return r.NewScope(value).PrimaryKeyZero()
}

// RecordNotFound check if returning ErrRecordNotFound error
func (r *repository) RecordNotFound() bool {
	for _, err := range r.GetErrors() {
		if err == ErrRecordNotFound {
			return true
		}
	}
	return false
}

// CreateTable create table for models
func (r *repository) CreateTable(models ...interface{}) Repository {
	db := r.Unscoped()
	for _, model := range models {
		db = db.NewScope(model).createTable().db
	}
	return db
}

// DropTable drop table for models
func (r *repository) DropTable(values ...interface{}) Repository {
	db := r.Clone()
	for _, value := range values {
		if tableName, ok := value.(string); ok {
			db = db.Table(tableName)
		}

		db = db.NewScope(value).dropTable().db
	}
	return db
}

// DropTableIfExists drop table if it is exist
func (r *repository) DropTableIfExists(values ...interface{}) Repository {
	db := r.Clone()
	for _, value := range values {
		if r.HasTable(value) {
			db.AddError(r.DropTable(value).Error())
		}
	}
	return db
}

// HasTable check has table or not
func (r *repository) HasTable(value interface{}) bool {
	var (
		scope     = r.NewScope(value)
		tableName string
	)

	if name, ok := value.(string); ok {
		tableName = name
	} else {
		tableName = scope.TableName()
	}

	has := scope.Dialect().HasTable(tableName)
	r.AddError(scope.db.Error())
	return has
}

// AutoMigrate run auto migration for given models, will only add missing fields, won't delete/change current data
func (r *repository) AutoMigrate(values ...interface{}) Repository {
	db := r.Unscoped()
	for _, value := range values {
		db = db.NewScope(value).autoMigrate().db
	}
	return db
}

// ModifyColumn modify column to type
func (r *repository) ModifyColumn(column string, typ string) Repository {
	scope := r.NewScope(r.value)
	scope.modifyColumn(column, typ)
	return scope.db
}

// DropColumn drop a column
func (r *repository) DropColumn(column string) Repository {
	scope := r.NewScope(r.value)
	scope.dropColumn(column)
	return scope.db
}

// AddIndex add index for columns with given name
func (r *repository) AddIndex(indexName string, columns ...string) Repository {
	scope := r.Unscoped().NewScope(r.value)
	scope.addIndex(false, indexName, columns...)
	return scope.db
}

// AddUniqueIndex add unique index for columns with given name
func (r *repository) AddUniqueIndex(indexName string, columns ...string) Repository {
	scope := r.Unscoped().NewScope(r.value)
	scope.addIndex(true, indexName, columns...)
	return scope.db
}

// RemoveIndex remove index with name
func (r *repository) RemoveIndex(indexName string) Repository {
	scope := r.NewScope(r.value)
	scope.removeIndex(indexName)
	return scope.db
}

// AddForeignKey Add foreign key to the given scope, e.g:
//     db.Model(&User{}).AddForeignKey("city_id", "cities(id)", "RESTRICT", "RESTRICT")
func (r *repository) AddForeignKey(field string, dest string, onDelete string, onUpdate string) Repository {
	scope := r.NewScope(r.value)
	scope.addForeignKey(field, dest, onDelete, onUpdate)
	return scope.db
}

// RemoveForeignKey Remove foreign key from the given scope, e.g:
//     db.Model(&User{}).RemoveForeignKey("city_id", "cities(id)")
func (r *repository) RemoveForeignKey(field string, dest string) Repository {
	scope := r.Clone().NewScope(r.value)
	scope.removeForeignKey(field, dest)
	return scope.db
}

// Association start `Association Mode` to handler relations things easir in that mode, refer: https://jinzhu.github.io/gorm/associations.html#association-mode
func (r *repository) Association(column string) *Association {
	var err error
	var scope = r.Set("gorm:association:source", r.value).NewScope(r.value)

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
func (r *repository) Preload(column string, conditions ...interface{}) Repository {
	return r.Clone().Search().Preload(column, conditions...).db
}

// Set set setting by name, which could be used in callbacks, will clone a new db, and update its setting
func (r *repository) Set(name string, value interface{}) Repository {
	return r.Clone().InstantSet(name, value)
}

// InstantSet instant set setting, will affect current db
func (r *repository) InstantSet(name string, value interface{}) Repository {
	r.values[name] = value
	return r
}

// Get get setting by name
func (r *repository) Get(name string) (value interface{}, ok bool) {
	value, ok = r.values[name]
	return
}

// SetJoinTableHandler set a model's join table handler for a relation
func (r *repository) SetJoinTableHandler(source interface{}, column string, handler JoinTableHandlerInterface) {
	scope := r.NewScope(source)
	for _, field := range scope.GetModelStruct().StructFields {
		if field.Name == column || field.DBName == column {
			if many2many := field.TagSettings["MANY2MANY"]; many2many != "" {
				source := (&Scope{Value: source}).GetModelStruct().ModelType
				destination := (&Scope{Value: reflect.New(field.Struct.Type).Interface()}).GetModelStruct().ModelType
				handler.Setup(field.Relationship, many2many, source, destination)
				field.Relationship.JoinTableHandler = handler
				if table := handler.Table(r); scope.Dialect().HasTable(table) {
					r.Table(table).AutoMigrate(handler)
				}
			}
		}
	}
}

// AddError add error to the db
func (r *repository) AddError(err error) error {
	if err != nil {
		if err != ErrRecordNotFound {
			if r.logMode == 0 {
				go r.Print(fileWithLineNum(), err)
			} else {
				r.Log(err)
			}

			errors := Errors(r.GetErrors())
			errors = errors.Add(err)
			if len(errors) > 1 {
				err = errors
			}
		}

		r.SetError(err)
	}
	return err
}

// GetErrors get happened errors from the db
func (r *repository) GetErrors() []error {
	if errs, ok := r.Error().(Errors); ok {
		return errs
	} else if r.Error() != nil {
		return []error{r.Error()}
	}
	return []error{}
}

func (r *repository) Value() interface{} {
	return r.value
}

func (r *repository) SetValue(v interface{}) Repository {
	r.value = v
	return r
}


func (r *repository) Error() error {
	return r.err
}

func (r *repository) SetError(err error) Repository {
	r.err = err
	return r
}

func (r *repository) RowsAffected() int64 {
	return r.rowsAffected
}

func (r *repository) SetRowsAffected(row int64) Repository {
	r.rowsAffected = row
	return r
}

func (r *repository) Search() *Search {
	return r.search
}

func (r *repository) SetSearch(search *Search) Repository {
	r.search = search
	return r
}

func (r *repository) Parent() Repository {
	return r.parent
}

func (r *repository) SetParent(p Repository) Repository {
	r.parent = p
	return r
}

func (r *repository) SQLCommonDB() SQLCommon {
	return r.db
}

func (r *repository) SetSQLCommonDB(sc SQLCommon) Repository {
	r.db = sc
	return r
}

func (r *repository) Callbacks() *Callback {
	return r.callbacks
}

func (r *repository) SetCallbacks(cb *Callback) Repository {
	r.callbacks = cb
	return r
}

func (r *repository) IsSingularTable() bool {
	return r.singularTable
}

func (r *repository) SetIsSingularTable(singularTable bool) Repository {
	r.singularTable = singularTable
	return r
}
func (r *repository) Values() map[string]interface{} {
	return r.values
}

func (r *repository) SetValues(vals map[string]interface{}) Repository {
	r.values = vals
	return r
}

func (r *repository) SetDialect(d Dialect) Repository {
	r.dialect = d
	return r
}


////////////////////////////////////////////////////////////////////////////////
// Private Methods For DB
////////////////////////////////////////////////////////////////////////////////

func (r *repository) Clone() Repository {
	db := &repository{
		db:                r.db,
		parent:            r.parent,
		logger:            r.logger,
		logMode:           r.logMode,
		values:            map[string]interface{}{},
		value:             r.value,
		err:               r.Error(),
		blockGlobalUpdate: r.blockGlobalUpdate,
		dialect:           newDialect(r.dialect.GetName(), r.db),
	}

	for key, value := range r.values {
		db.values[key] = value
	}

	if r.search == nil {
		db.search = &Search{limit: -1, offset: -1}
	} else {
		db.search = r.Search().clone()
	}

	db.Search().db = db
	return db
}

func (r *repository) Print(v ...interface{}) {
	r.logger.Print(v...)
}

func (r *repository) Log(v ...interface{}) {
	if r != nil && r.logMode == 2 {
		r.Print(append([]interface{}{"log", fileWithLineNum()}, v...)...)
	}
}

func (r *repository) Slog(sql string, t time.Time, vars ...interface{}) {
	if r.logMode == 2 {
		r.Print("sql", fileWithLineNum(), NowFunc().Sub(t), sql, vars, r.RowsAffected())
	}
}
