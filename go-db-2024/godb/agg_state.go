package godb

import (
	"fmt"
)

var DEBUGAGGSTATE = false

func DebugAggState(format string, a ...any) (int, error) {
	if DEBUGAGGSTATE || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	alias    string
	expr     Expr
	sumInt   int64
	sumFloat float64
	sumStr   string
}

func (a *SumAggState) Copy() AggState {
	// TODO: some code goes here
	return &SumAggState{a.alias, a.expr, a.sumInt, a.sumFloat, a.sumStr}
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.sumInt = 0
	a.sumFloat = 0
	a.sumStr = ""
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	dbValue, err := a.expr.EvalExpr(t)
	if err != nil {
		DebugAggState("Got err: %v", err)
	}

	switch dbType := dbValue.(type) {
	case IntField:
		a.sumInt += dbType.Value
	case FloatField:
		a.sumFloat += dbType.Value
	case StringField:
		a.sumStr += dbType.Value
	}
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{[]FieldType{{a.alias, "", a.expr.GetExprType().Ftype}}}
}

func (a *SumAggState) Finalize() *Tuple {
	// TODO: some code goes here
	var f DBValue
	switch a.expr.GetExprType().Ftype {
	case IntType:
		f = IntField{a.sumInt}
	case FloatType:
		f = FloatField{a.sumFloat}
	case StringType:
		f = StringField{a.sumStr}
	}
	return &Tuple{*a.GetTupleDesc(), []DBValue{f}, nil}
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	// TODO: some code goes here
	alias    string
	expr     Expr
	sum      int64
	sumFloat float64
	count    int
}

func (a *AvgAggState) Copy() AggState {
	// TODO: some code goes here
	return &AvgAggState{a.alias, a.expr, a.sum, a.sumFloat, a.count}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	if expr.GetExprType().Ftype == StringType {
		return GoDBError{TypeMismatchError, "Shouldn't be averaging a string column"}
	}
	a.sumFloat = 0
	a.sum = 0
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	a.count++
	dbValue, err := a.expr.EvalExpr(t)
	if err != nil {
		DebugAggState("Got err: %v", err)
	}

	switch dbType := dbValue.(type) {
	case IntField:
		a.sum += dbType.Value
	case FloatField:
		a.sumFloat += dbType.Value
	case StringField:
		DebugAggState("Shouldn't be average a string value!")
	}
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{[]FieldType{{a.alias, "", a.expr.GetExprType().Ftype}}}
}

func (a *AvgAggState) Finalize() *Tuple {
	// TODO: some code goes here
	var f DBValue
	switch a.expr.GetExprType().Ftype {
	case IntType:
		f = IntField{a.sum / int64(a.count)}
	case FloatType:
		f = FloatField{a.sumFloat / float64(a.count)}
	}
	return &Tuple{*a.GetTupleDesc(), []DBValue{f}, nil}
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	// TODO: some code goes here
	alias      string
	expr       Expr
	addedValue bool
	maxInt     int64
	maxFloat   float64
	maxStr     string
}

func (a *MaxAggState) Copy() AggState {
	// TODO: some code goes here
	return &MaxAggState{a.alias, a.expr, a.addedValue, a.maxInt, a.maxFloat, a.maxStr}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.addedValue = false
	a.maxInt = 0
	a.maxFloat = 0
	a.maxStr = ""
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	dbValue, err := a.expr.EvalExpr(t)
	if err != nil {
		DebugAggState("Got err: %v", err)
	}

	switch dbType := dbValue.(type) {
	case IntField:
		if a.addedValue {
			a.maxInt = max(a.maxInt, dbType.Value)
		} else {
			a.maxInt = dbType.Value
			a.addedValue = true
		}
	case FloatField:
		if a.addedValue {
			a.maxFloat = max(a.maxFloat, dbType.Value)
		} else {
			a.maxFloat = dbType.Value
			a.addedValue = true
		}
	case StringField:
		if a.addedValue {
			a.maxStr = max(a.maxStr, dbType.Value)
		} else {
			a.maxStr = dbType.Value
			a.addedValue = true
		}
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{[]FieldType{{a.alias, "", a.expr.GetExprType().Ftype}}}
}

func (a *MaxAggState) Finalize() *Tuple {
	// TODO: some code goes here
	var f DBValue
	switch a.expr.GetExprType().Ftype {
	case IntType:
		f = IntField{a.maxInt}
	case FloatType:
		f = FloatField{a.maxFloat}
	case StringType:
		f = StringField{a.maxStr}
	}
	return &Tuple{*a.GetTupleDesc(), []DBValue{f}, nil}
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	// TODO: some code goes here
	alias      string
	expr       Expr
	addedValue bool
	minInt     int64
	minFloat   float64
	minStr     string
}

func (a *MinAggState) Copy() AggState {
	// TODO: some code goes here
	return &MinAggState{a.alias, a.expr, a.addedValue, a.minInt, a.minFloat, a.minStr}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.addedValue = false
	a.minInt = 0
	a.minFloat = 0
	a.minStr = ""
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	dbValue, err := a.expr.EvalExpr(t)
	if err != nil {
		DebugAggState("Got err: %v", err)
	}

	switch dbType := dbValue.(type) {
	case IntField:
		if a.addedValue {
			a.minInt = min(a.minInt, dbType.Value)
		} else {
			a.minInt = dbType.Value
			a.addedValue = true
		}
	case FloatField:
		if a.addedValue {
			a.minFloat = min(a.minFloat, dbType.Value)
		} else {
			a.minFloat = dbType.Value
			a.addedValue = true
		}
	case StringField:
		if a.addedValue {
			a.minStr = min(a.minStr, dbType.Value)
		} else {
			a.minStr = dbType.Value
			a.addedValue = true
		}
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{[]FieldType{{a.alias, "", a.expr.GetExprType().Ftype}}}
}

func (a *MinAggState) Finalize() *Tuple {
	// TODO: some code goes here
	var f DBValue
	switch a.expr.GetExprType().Ftype {
	case IntType:
		f = IntField{a.minInt}
	case FloatType:
		f = FloatField{a.minFloat}
	case StringType:
		f = StringField{a.minStr}
	}
	return &Tuple{*a.GetTupleDesc(), []DBValue{f}, nil}
}
