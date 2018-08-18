package gorm

import (
	"fmt"
)

type Search struct {
	db               Repository
	whereConditions  []map[string]interface{}
	orConditions     []map[string]interface{}
	notConditions    []map[string]interface{}
	havingConditions []map[string]interface{}
	joinConditions   []map[string]interface{}
	initAttrs        []interface{}
	assignAttrs      []interface{}
	selects          map[string]interface{}
	omits            []string
	orders           []interface{}
	preload          []searchPreload
	offset           interface{}
	limit            interface{}
	group            string
	tableName        string
	raw              bool
	Unscoped         bool
	ignoreOrderQuery bool
}

type searchPreload struct {
	schema     string
	conditions []interface{}
}

func (s *Search) clone() *Search {
	clone := *s
	return &clone
}

func (s *Search) Where(query interface{}, values ...interface{}) *Search {
	s.whereConditions = append(s.whereConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *Search) Not(query interface{}, values ...interface{}) *Search {
	s.notConditions = append(s.notConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *Search) Or(query interface{}, values ...interface{}) *Search {
	s.orConditions = append(s.orConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *Search) Attrs(attrs ...interface{}) *Search {
	s.initAttrs = append(s.initAttrs, toSearchableMap(attrs...))
	return s
}

func (s *Search) Assign(attrs ...interface{}) *Search {
	s.assignAttrs = append(s.assignAttrs, toSearchableMap(attrs...))
	return s
}

func (s *Search) Order(value interface{}, reorder ...bool) *Search {
	if len(reorder) > 0 && reorder[0] {
		s.orders = []interface{}{}
	}

	if value != nil && value != "" {
		s.orders = append(s.orders, value)
	}
	return s
}

func (s *Search) Select(query interface{}, args ...interface{}) *Search {
	s.selects = map[string]interface{}{"query": query, "args": args}
	return s
}

func (s *Search) Omit(columns ...string) *Search {
	s.omits = columns
	return s
}

func (s *Search) Limit(limit interface{}) *Search {
	s.limit = limit
	return s
}

func (s *Search) Offset(offset interface{}) *Search {
	s.offset = offset
	return s
}

func (s *Search) Group(query string) *Search {
	s.group = s.getInterfaceAsSQL(query)
	return s
}

func (s *Search) Having(query interface{}, values ...interface{}) *Search {
	if val, ok := query.(*Expression); ok {
		s.havingConditions = append(s.havingConditions, map[string]interface{}{"query": val.expr, "args": val.args})
	} else {
		s.havingConditions = append(s.havingConditions, map[string]interface{}{"query": query, "args": values})
	}
	return s
}

func (s *Search) Joins(query string, values ...interface{}) *Search {
	s.joinConditions = append(s.joinConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *Search) Preload(schema string, values ...interface{}) *Search {
	var preloads []searchPreload
	for _, preload := range s.preload {
		if preload.schema != schema {
			preloads = append(preloads, preload)
		}
	}
	preloads = append(preloads, searchPreload{schema, values})
	s.preload = preloads
	return s
}

func (s *Search) Raw(b bool) *Search {
	s.raw = b
	return s
}

func (s *Search) unscoped() *Search {
	s.Unscoped = true
	return s
}

func (s *Search) Table(name string) *Search {
	s.tableName = name
	return s
}

func (s *Search) getInterfaceAsSQL(value interface{}) (str string) {
	switch value.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		str = fmt.Sprintf("%v", value)
	default:
		s.db.AddError(ErrInvalidSQL)
	}

	if str == "-1" {
		return ""
	}
	return
}
