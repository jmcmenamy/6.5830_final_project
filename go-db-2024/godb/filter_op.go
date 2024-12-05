package godb

import "fmt"

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

var DEBUGFILTER = false

func DebugFilter(format string, a ...any) (int, error) {
	if DEBUGFILTER || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

func (f *Filter) Statistics() map[string]map[string]float64 {
	return f.child.Statistics()
}

// Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.child.Descriptor()
}

// Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter, err := f.child.Iterator(tid)
	if err != nil {
		DebugFilter("Got err creating iterator: %v", err)
		return nil, err
	}

	getTuple := func() (*Tuple, error) {
		for {

			// if nothing to return
			if iter == nil {
				return nil, nil
			}

			tup, err := iter()
			if err != nil {
				DebugFilter("Got err getting tuple: %v", err)
				return nil, err
			}

			// EOF, return nothing
			if tup == nil {
				iter = nil
				return nil, nil
			}

			// evaluate left and right sides
			v1, err := f.left.EvalExpr(tup)
			if err != nil {
				DebugFilter("Got err getting tuple: %v", err)
				return nil, err
			}

			v2, err := f.right.EvalExpr(tup)
			if err != nil {
				DebugFilter("Got err getting tuple: %v", err)
				return nil, err
			}

			// return if passes filtering
			if v1.EvalPred(v2, f.op) {
				// fmt.Printf("%v passes filtering \n", tup)
				return tup, nil
			}

			// iterate to next value
		}
	}
	return getTuple, nil // replace me
}
