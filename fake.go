// Copyright 2018 John Deng (hi.devops.io@gmail.com).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gorm

import (
	"database/sql"
	"time"
	"fmt"
	"errors"
	"github.com/jinzhu/copier"
)

type Mocker interface {
	Mock(method string, data interface{}) *FakeRepository
	Expect(err error)
}

// DB contains information for current db connection
type FakeRepository struct {
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
	mockData      map[string]interface{}
}

// New clone a new db connection without search conditions
func (r *FakeRepository) New() Repository {
	clone := r.Clone()
	clone.SetSearch(nil)
	clone.SetValue(nil)
	return clone
}

// Close close current db connection.  If database connection is not an io.Closer, returns an error.
func (r *FakeRepository) Close() error {
	if db, ok := r.Parent().SQLCommonDB().(closer); ok {
		return db.Close()
	}
	return errors.New("can't close current db")
}

// DB get `*sql.DB` from current connection
// If the underlying database connection is not a *sql.DB, returns nil
func (r *FakeRepository) SqlDB() *sql.DB {
	db, _ := r.db.(*sql.DB)
	return db
}

// CommonDB return the underlying `*sql.DB` or `*sql.Tx` instance, mainly intended to allow coexistence with legacy non-GORM code.
func (r *FakeRepository) CommonDB() SQLCommon {
	return r.db
}

// Dialect get dialect
func (r *FakeRepository) Dialect() Dialect {
	return r.dialect
}

// Callback return `Callbacks` container, you could add/change/delete callbacks with it
//     db.Callback().Create().Register("update_created_at", updateCreated)
// Refer https://jinzhu.github.io/gorm/development.html#callbacks
func (r *FakeRepository) Callback() *Callback {
	r.parent.SetCallbacks(r.parent.Callbacks().clone())
	return r.parent.Callbacks()
}

// SetLogger replace default logger
func (r *FakeRepository) SetLogger(log Logger) Repository {
	r.logger = log
	return r
}

// LogMode set log mode, `true` for detailed logs, `false` for no log, default, will only print error logs
func (r *FakeRepository) LogMode(enable bool) Repository {
	if enable {
		r.logMode = 2
	} else {
		r.logMode = 1
	}
	return r
}

// BlockGlobalUpdate if true, generates an error on update/delete without where clause.
// This is to prevent eventual error with empty objects updates/deletions
func (r *FakeRepository) BlockGlobalUpdate(enable bool) Repository {
	r.blockGlobalUpdate = enable
	return r
}

// HasBlockGlobalUpdate return state of block
func (r *FakeRepository) HasBlockGlobalUpdate() bool {
	return r.blockGlobalUpdate
}

// SingularTable use singular table by default
func (r *FakeRepository) SingularTable(enable bool) {
	r.parent.SetIsSingularTable(enable)
}

// NewScope create a scope for current operation
func (r *FakeRepository) NewScope(value interface{}) *Scope {
	dbClone := r.Clone()
	dbClone.SetValue(value)
	scope := &Scope{db: dbClone, Search: dbClone.Search().clone(), Value: value}
	return scope
}

// QueryExpr returns the query as expr object
func (r *FakeRepository) QueryExpr() *Expression {
	scope := r.NewScope(r.value)
	scope.InstanceSet("skip_bindvar", true)
	scope.prepareQuerySQL()

	return Expr(scope.SQL, scope.SQLVars...)
}

// SubQuery returns the query as sub query
func (r *FakeRepository) SubQuery() *Expression {
	scope := r.NewScope(r.value)
	scope.InstanceSet("skip_bindvar", true)
	scope.prepareQuerySQL()

	return Expr(fmt.Sprintf("(%v)", scope.SQL), scope.SQLVars...)
}

// Where return a new relation, filter records with given conditions, accepts `map`, `struct` or `string` as conditions, refer http://jinzhu.github.io/gorm/crud.html#query
func (r *FakeRepository) Where(query interface{}, args ...interface{}) Repository {
	return r
}

// Or filter records that match before conditions or this one, similar to `Where`
func (r *FakeRepository) Or(query interface{}, args ...interface{}) Repository {
	return r
}

// Not filter records that don't match current conditions, similar to `Where`
func (r *FakeRepository) Not(query interface{}, args ...interface{}) Repository {
	return r
}

// Limit specify the number of records to be retrieved
func (r *FakeRepository) Limit(limit interface{}) Repository {
	return r
}

// Offset specify the number of records to skip before starting to return the records
func (r *FakeRepository) Offset(offset interface{}) Repository {
	return r
}

// Order specify order when retrieve records from database, set reorder to `true` to overwrite defined conditions
//     db.Order("name DESC")
//     db.Order("name DESC", true) // reorder
//     db.Order(gorm.Expr("name = ? DESC", "first")) // sql expression
func (r *FakeRepository) Order(value interface{}, reorder ...bool) Repository {
	return r
}

// Select specify fields that you want to retrieve from database when querying, by default, will select all fields;
// When creating/updating, specify fields that you want to save to database
func (r *FakeRepository) Select(query interface{}, args ...interface{}) Repository {
	return r
}

// Omit specify fields that you want to ignore when saving to database for creating, updating
func (r *FakeRepository) Omit(columns ...string) Repository {
	return r
}

// Group specify the group method on the find
func (r *FakeRepository) Group(query string) Repository {
	return r
}

// Having specify HAVING conditions for GROUP BY
func (r *FakeRepository) Having(query interface{}, values ...interface{}) Repository {
	return r
}

// Joins specify Joins conditions
//     db.Joins("JOIN emails ON emails.user_id = users.id AND emails.email = ?", "jinzhu@example.org").Find(&user)
func (r *FakeRepository) Joins(query string, args ...interface{}) Repository {
	return r
}

func (r *FakeRepository) Scopes(funcs ...func(Repository) Repository) Repository {
	return r
}

// Unscoped return all record including deleted record, refer Soft Delete https://jinzhu.github.io/gorm/crud.html#soft-delete
func (r *FakeRepository) Unscoped() Repository {
	return r
}

// Attrs initialize struct with argument if record not found with `FirstOrInit` https://jinzhu.github.io/gorm/crud.html#firstorinit or `FirstOrCreate` https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (r *FakeRepository) Attrs(attrs ...interface{}) Repository {
	return r
}

// Assign assign result with argument regardless it is found or not with `FirstOrInit` https://jinzhu.github.io/gorm/crud.html#firstorinit or `FirstOrCreate` https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (r *FakeRepository) Assign(attrs ...interface{}) Repository {
	return r
}

// First find first record that match given conditions, order by primary key
func (r *FakeRepository) First(out interface{}, where ...interface{}) Repository {
	r.copyData("First", out)
	return r
}

// Take return a record that match given conditions, the order will depend on the database implementation
func (r *FakeRepository) Take(out interface{}, where ...interface{}) Repository {
	r.copyData("Take", out)
	return r
}

// Last find last record that match given conditions, order by primary key
func (r *FakeRepository) Last(out interface{}, where ...interface{}) Repository {
	r.copyData("Last", out)
	return r
}

// Find find records that match given conditions
func (r *FakeRepository) Find(out interface{}, where ...interface{}) Repository {
	r.copyData("Find", out)
	return r
}

// Scan scan value to a struct
func (r *FakeRepository) Scan(dest interface{}) Repository {
	r.copyData("Scan", dest)
	return r
}

// Row return `*sql.Row` with given conditions
func (r *FakeRepository) Row() *sql.Row {
	return nil
}

// Rows return `*sql.Rows` with given conditions
func (r *FakeRepository) Rows() (*sql.Rows, error) {
	return nil, nil
}

// ScanRows scan `*sql.Rows` to give struct
func (r *FakeRepository) ScanRows(rows *sql.Rows, result interface{}) error {
	return nil
}

// Pluck used to query single column from a model as a map
//     var ages []int64
//     db.Find(&users).Pluck("age", &ages)
func (r *FakeRepository) Pluck(column string, value interface{}) Repository {
	return r
}

// Count get how many records for a model
func (r *FakeRepository) Count(value interface{}) Repository {
	return r
}

// Related get related associations
func (r *FakeRepository) Related(value interface{}, foreignKeys ...string) Repository {
	return r
}

// FirstOrInit find first matched record or initialize a new one with given conditions (only works with struct, map conditions)
// https://jinzhu.github.io/gorm/crud.html#firstorinit
func (r *FakeRepository) FirstOrInit(out interface{}, where ...interface{}) Repository {
	r.copyData("FirstOrInit", out)
	return r
}

// FirstOrCreate find first matched record or create a new one with given conditions (only works with struct, map conditions)
// https://jinzhu.github.io/gorm/crud.html#firstorcreate
func (r *FakeRepository) FirstOrCreate(out interface{}, where ...interface{}) Repository {
	r.copyData("FirstOrCreate", out)
	return r
}

// Update update attributes with callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (r *FakeRepository) Update(attrs ...interface{}) Repository {
	return r
}

// Updates update attributes with callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (r *FakeRepository) Updates(values interface{}, ignoreProtectedAttrs ...bool) Repository {
	return r
}

// UpdateColumn update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (r *FakeRepository) UpdateColumn(attrs ...interface{}) Repository {
	return r
}

// UpdateColumns update attributes without callbacks, refer: https://jinzhu.github.io/gorm/crud.html#update
func (r *FakeRepository) UpdateColumns(values interface{}) Repository {
	return r
}

// Save update value in database, if the value doesn't have primary key, will insert it
func (r *FakeRepository) Save(value interface{}) Repository {
	return r
}

// Create insert the value into database
func (r *FakeRepository) Create(value interface{}) Repository {
	return r
}

// Delete delete value match given conditions, if the value has primary key, then will including the primary key as condition
func (r *FakeRepository) Delete(value interface{}, where ...interface{}) Repository {
	return r
}

// Raw use raw sql as conditions, won't run it unless invoked by other methods
//    db.Raw("SELECT name, age FROM users WHERE name = ?", 3).Scan(&result)
func (r *FakeRepository) Raw(sql string, values ...interface{}) Repository {
	return r
}

// Exec execute raw sql
func (r *FakeRepository) Exec(sql string, values ...interface{}) Repository {
	return r
}

// Model specify the model you would like to run db operations
//    // update all users's name to `hello`
//    db.Model(&User{}).Update("name", "hello")
//    // if user's primary key is non-blank, will use it as condition, then will only update the user's name to `hello`
//    db.Model(&user).Update("name", "hello")
func (r *FakeRepository) Model(value interface{}) Repository {
	return r
}

// Table specify the table you would like to run db operations
func (r *FakeRepository) Table(name string) Repository {
	return r
}

// Debug start debug mode
func (r *FakeRepository) Debug() Repository {
	return r
}

// Begin begin a transaction
func (r *FakeRepository) Begin() Repository {
	return r
}

// Commit commit a transaction
func (r *FakeRepository) Commit() Repository {
	return r
}

// Rollback rollback a transaction
func (r *FakeRepository) Rollback() Repository {
	return r
}

// NewRecord check if value's primary key is blank
func (r *FakeRepository) NewRecord(value interface{}) bool {
	return false
}

// RecordNotFound check if returning ErrRecordNotFound error
func (r *FakeRepository) RecordNotFound() bool {
	return false
}

// CreateTable create table for models
func (r *FakeRepository) CreateTable(models ...interface{}) Repository {
	return r
}

// DropTable drop table for models
func (r *FakeRepository) DropTable(values ...interface{}) Repository {
	return r
}

// DropTableIfExists drop table if it is exist
func (r *FakeRepository) DropTableIfExists(values ...interface{}) Repository {
	return r
}

// HasTable check has table or not
func (r *FakeRepository) HasTable(value interface{}) bool {
	return false
}

// AutoMigrate run auto migration for given models, will only add missing fields, won't delete/change current data
func (r *FakeRepository) AutoMigrate(values ...interface{}) Repository {
	return r
}

// ModifyColumn modify column to type
func (r *FakeRepository) ModifyColumn(column string, typ string) Repository {
	return r
}

// DropColumn drop a column
func (r *FakeRepository) DropColumn(column string) Repository {
	return r
}

// AddIndex add index for columns with given name
func (r *FakeRepository) AddIndex(indexName string, columns ...string) Repository {
	return r
}

// AddUniqueIndex add unique index for columns with given name
func (r *FakeRepository) AddUniqueIndex(indexName string, columns ...string) Repository {
	return r
}

// RemoveIndex remove index with name
func (r *FakeRepository) RemoveIndex(indexName string) Repository {
	return r
}

// AddForeignKey Add foreign key to the given scope, e.g:
//     db.Model(&User{}).AddForeignKey("city_id", "cities(id)", "RESTRICT", "RESTRICT")
func (r *FakeRepository) AddForeignKey(field string, dest string, onDelete string, onUpdate string) Repository {
	return r
}

// RemoveForeignKey Remove foreign key from the given scope, e.g:
//     db.Model(&User{}).RemoveForeignKey("city_id", "cities(id)")
func (r *FakeRepository) RemoveForeignKey(field string, dest string) Repository {
	return r
}

// Association start `Association Mode` to handler relations things easir in that mode, refer: https://jinzhu.github.io/gorm/associations.html#association-mode
func (r *FakeRepository) Association(column string) *Association {
	return nil
}

// Preload preload associations with given conditions
//    db.Preload("Orders", "state NOT IN (?)", "cancelled").Find(&users)
func (r *FakeRepository) Preload(column string, conditions ...interface{}) Repository {
	return r
}

// Set set setting by name, which could be used in callbacks, will clone a new db, and update its setting
func (r *FakeRepository) Set(name string, value interface{}) Repository {
	return r
}

// InstantSet instant set setting, will affect current db
func (r *FakeRepository) InstantSet(name string, value interface{}) Repository {
	return r
}

// Get get setting by name
func (r *FakeRepository) Get(name string) (value interface{}, ok bool) {
	value, ok = nil, false
	return
}

// SetJoinTableHandler set a model's join table handler for a relation
func (r *FakeRepository) SetJoinTableHandler(source interface{}, column string, handler JoinTableHandlerInterface) {
}

// AddError add error to the db
func (r *FakeRepository) AddError(err error) error {
	if err != nil {
		if err != ErrRecordNotFound {
			if r.logMode == 0 {
				go r.Print(fileWithLineNum(), err)
			} else {
				r.Log(err)
			}

			errs := Errors(r.GetErrors())
			errs = errs.Add(err)
			if len(errs) > 1 {
				err = errs
			}
		}

		r.SetError(err)
	}
	return err
}

// GetErrors get happened errors from the db
func (r *FakeRepository) GetErrors() []error {
	if errs, ok := r.Error().(Errors); ok {
		return errs
	} else if r.Error() != nil {
		return []error{r.Error()}
	}
	return []error{}
}

func (r *FakeRepository) Value() interface{} {
	return r.value
}

func (r *FakeRepository) SetValue(v interface{}) Repository {
	r.value = v
	return r
}

func (r *FakeRepository) Error() error {
	return r.err
}

func (r *FakeRepository) SetError(err error) Repository {
	r.err = err
	return r
}

func (r *FakeRepository) RowsAffected() int64 {
	return r.rowsAffected
}

func (r *FakeRepository) SetRowsAffected(row int64) Repository {
	r.rowsAffected = row
	return r
}

func (r *FakeRepository) Search() *Search {
	return r.search
}

func (r *FakeRepository) SetSearch(search *Search) Repository {
	r.search = search
	return r
}

func (r *FakeRepository) Parent() Repository {
	return r.parent
}

func (r *FakeRepository) SetParent(p Repository) Repository {
	r.parent = p
	return r
}

func (r *FakeRepository) SQLCommonDB() SQLCommon {
	return r.db
}

func (r *FakeRepository) SetSQLCommonDB(sc SQLCommon) Repository {
	r.db = sc
	return r
}

func (r *FakeRepository) Callbacks() *Callback {
	return r.callbacks
}

func (r *FakeRepository) SetCallbacks(cb *Callback) Repository {
	r.callbacks = cb
	return r
}

func (r *FakeRepository) IsSingularTable() bool {
	return r.singularTable
}

func (r *FakeRepository) SetIsSingularTable(singularTable bool) Repository {
	r.singularTable = singularTable
	return r
}
func (r *FakeRepository) Values() map[string]interface{} {
	return r.values
}

func (r *FakeRepository) SetValues(vals map[string]interface{}) Repository {
	r.values = vals
	return r
}

func (r *FakeRepository) SetDialect(d Dialect) Repository {
	r.dialect = d
	return r
}

////////////////////////////////////////////////////////////////////////////////
// Private Methods For DB
////////////////////////////////////////////////////////////////////////////////

func (r *FakeRepository) Clone() Repository {
	db := &FakeRepository{
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

func (r *FakeRepository) Print(v ...interface{}) {
	r.logger.Print(v...)
}

func (r *FakeRepository) Log(v ...interface{}) {
	if r != nil && r.logMode == 2 {
		r.Print(append([]interface{}{"log", fileWithLineNum()}, v...)...)
	}
}

func (r *FakeRepository) Slog(sql string, t time.Time, vars ...interface{}) {
	if r.logMode == 2 {
		r.Print("sql", fileWithLineNum(), NowFunc().Sub(t), sql, vars, r.RowsAffected())
	}
}

func (r *FakeRepository) Mock(method string, data interface{}) *FakeRepository {
	if r.mockData == nil {
		r.mockData = make(map[string]interface{})
	}
	r.mockData[method] = data

	return r
}

func (r *FakeRepository) Expect(err error) {
	r.SetError(err)
}

func (r *FakeRepository) copyData(name string, out interface{})  {
	md := r.mockData[name]
	if md != nil {
		copier.Copy(out, md)
	}
}
