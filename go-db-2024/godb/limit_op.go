package godb

import (
	"fmt"
)

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

var DEBUGLIMIT = false

func DebugLimit(format string, a ...any) (int, error) {
	if DEBUGLIMIT || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child, lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	count := 0
	reachedLimit := false
	return func() (*Tuple, error) {
		if reachedLimit {
			return nil, nil
		}

		childTup, err := childIter()

		if err != nil {
			return nil, err
		}

		// no more tups
		if childTup == nil {
			return nil, nil
		}

		count++
		dbVal, err := l.limitTups.EvalExpr(childTup)
		if err != nil {
			return nil, err
		}

		// see if we've reached limit
		if dbVal.EvalPred(IntField{int64(count)}, OpLt) {
			reachedLimit = true
			return nil, nil
		}

		return childTup, nil
	}, nil
}
