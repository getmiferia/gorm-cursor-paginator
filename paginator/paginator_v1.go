package paginator

import (
	"fmt"
	"github.com/getmiferia/gorm-cursor-paginator/v2/cursor"
	"reflect"
	"strings"
)

/*
 * Created by Ashok Kumar Pant
 * Email: asokpant@gmail.com
 * Created on 3/14/23.
 */

// NewSqlPaginator creates paginator
func NewSqlPaginator(opts ...Option) *SqlPaginator {
	p := &SqlPaginator{}
	for _, opt := range append([]Option{&defaultConfig}, opts...) {
		opt.Apply1(p)
	}
	return p
}

// SqlPaginator a builder doing pagination
type SqlPaginator struct {
	Paginator
}

// SetRules sets paging rules
func (p *SqlPaginator) SetRules(rules ...Rule) {
	p.rules = make([]Rule, len(rules))
	copy(p.rules, rules)
}

// SetKeys sets paging keys
func (p *SqlPaginator) SetKeys(keys ...string) {
	rules := make([]Rule, len(keys))
	for i, key := range keys {
		rules[i] = Rule{
			Key: key,
		}
	}
	p.SetRules(rules...)
}

// SetLimit sets paging limit
func (p *SqlPaginator) SetLimit(limit int) {
	p.limit = limit
}

// SetOrder sets paging order
func (p *SqlPaginator) SetOrder(order Order) {
	p.order = order
}

// SetAfterCursor sets paging after cursor
func (p *SqlPaginator) SetAfterCursor(afterCursor string) {
	p.cursor.After = &afterCursor
}

// SetBeforeCursor sets paging before cursor
func (p *SqlPaginator) SetBeforeCursor(beforeCursor string) {
	p.cursor.Before = &beforeCursor
}

// AppendPaginationQuery appends pagination query to db
func (p *SqlPaginator) AppendPaginationQuery(dest interface{}) (result string, err error) {
	fields, err := p.DecodeCursor(dest)
	if err != nil {
		return
	}
	result = p.AppendPagingQuery(fields)
	return result, nil
}

func (p *SqlPaginator) GetPaginationQuery(dest interface{}) (string, error) {
	stmt1, err := p.AppendPaginationQuery(dest)
	if err != nil {
		return "", err
	}
	sql := strings.TrimSpace(stmt1)
	return sql, nil
}

// GetCursor  gets new cursor from dest
func (p *SqlPaginator) GetCursor(dest interface{}) (c Cursor, err error) {
	// dest must be a pointer type or gorm will panic above
	elems := reflect.ValueOf(dest).Elem()
	// only encode next cursor when elems is not empty slice
	if elems.Kind() == reflect.Slice && elems.Len() > 0 {
		hasMore := elems.Len() > p.limit
		if hasMore {
			elems.Set(elems.Slice(0, elems.Len()-1))
		}
		if p.isBackward() {
			elems.Set(reverse(elems))
		}
		if c, err = p.EncodeCursor(elems, hasMore); err != nil {
			return
		}
	}
	return
}

func (p *SqlPaginator) DecodeCursor(dest interface{}) (result []interface{}, err error) {
	if p.isForward() {
		if result, err = cursor.NewDecoder(p.getDecoderFields()).Decode(*p.cursor.After, dest); err != nil {
			err = ErrInvalidCursor
		}
	} else if p.isBackward() {
		if result, err = cursor.NewDecoder(p.getDecoderFields()).Decode(*p.cursor.Before, dest); err != nil {
			err = ErrInvalidCursor
		}
	}
	// replace null values
	for i := range result {
		if isNil(result[i]) {
			result[i] = p.rules[i].NULLReplacement
		}
	}
	return
}

func (p *SqlPaginator) isForward() bool {
	return p.cursor.After != nil
}

func (p *SqlPaginator) isBackward() bool {
	// forward take precedence over backward
	return !p.isForward() && p.cursor.Before != nil
}

func (p *SqlPaginator) AppendPagingQuery(fields []interface{}) string {
	stmt := ""
	if len(fields) > 0 {
		q := p.BuildCursorSQLQuery()
		args := p.BuildCursorSQLQueryArgs(fields)
		q = ExplainSQL(q, nil, `'`, args...)
		stmt = q
	}
	stmt = fmt.Sprintf("%s ORDER BY %s", stmt, p.BuildOrderSQL())
	stmt = fmt.Sprintf("%s LIMIT %d", stmt, p.limit+1)
	return stmt
}

func (p *SqlPaginator) BuildOrderSQL() string {
	orders := make([]string, len(p.rules))
	for i, rule := range p.rules {
		order := rule.Order
		if p.isBackward() {
			order = order.flip()
		}
		orders[i] = fmt.Sprintf("%s %s", rule.SQLRepr, order)
	}
	return strings.Join(orders, ", ")
}

func (p *SqlPaginator) BuildCursorSQLQuery() string {
	queries := make([]string, len(p.rules))
	query := ""
	for i, rule := range p.rules {
		operator := "<"
		if (p.isForward() && rule.Order == ASC) ||
			(p.isBackward() && rule.Order == DESC) {
			operator = ">"
		}
		queries[i] = fmt.Sprintf("%s%s %s ?", query, rule.SQLRepr, operator)
		query = fmt.Sprintf("%s%s = ? AND ", query, rule.SQLRepr)
	}
	// for exmaple:
	// a > 1 OR a = 1 AND b > 2 OR a = 1 AND b = 2 AND c > 3
	return strings.Join(queries, " OR ")
}

func (p *SqlPaginator) BuildCursorSQLQueryArgs(fields []interface{}) (args []interface{}) {
	for i := 1; i <= len(fields); i++ {
		args = append(args, fields[:i]...)
	}
	return
}

func (p *SqlPaginator) EncodeCursor(elems reflect.Value, hasMore bool) (result Cursor, err error) {
	encoder := cursor.NewEncoder(p.getEncoderFields())
	// encode after cursor
	if p.isBackward() || hasMore {
		c, err := encoder.Encode(elems.Index(elems.Len() - 1))
		if err != nil {
			return Cursor{}, err
		}
		result.After = &c
	}
	// encode before cursor
	if p.isForward() || (hasMore && p.isBackward()) {
		c, err := encoder.Encode(elems.Index(0))
		if err != nil {
			return Cursor{}, err
		}
		result.Before = &c
	}
	return
}

/* custom types */
func (p *SqlPaginator) getEncoderFields() []cursor.EncoderField {
	fields := make([]cursor.EncoderField, len(p.rules))
	for i, rule := range p.rules {
		fields[i].Key = rule.Key
		if rule.CustomType != nil {
			fields[i].Meta = rule.CustomType.Meta
		}
	}
	return fields
}

func (p *SqlPaginator) getDecoderFields() []cursor.DecoderField {
	fields := make([]cursor.DecoderField, len(p.rules))
	for i, rule := range p.rules {
		fields[i].Key = rule.Key
		if rule.CustomType != nil {
			fields[i].Type = &rule.CustomType.Type
		}
	}
	return fields
}
