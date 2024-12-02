package godb

import (
	"fmt"
	"sort"
)

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	// TODO: You may want to add additional fields here
	ascending []bool
	tuples    []*Tuple
}

var DEBUGORDER = false

func DebugOrder(format string, a ...any) (int, error) {
	if DEBUGORDER || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	if len(orderByFields) != len(ascending) {
		return nil, GoDBError{TypeMismatchError, fmt.Sprintf("Got wrong lengths %v %v", len(orderByFields), len(ascending))}
	}
	return &OrderBy{orderByFields, child, ascending, nil}, nil
}

func (o *OrderBy) Statistics() map[string]map[string]float64 {
	return o.child.Statistics()
}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor()
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (o *OrderBy) Sort(changes []*Tuple) {
	DebugOrder("got here")
	o.tuples = changes
	sort.Sort(o)
}

// Len is part of sort.Interface.
func (o *OrderBy) Len() int {
	return len(o.tuples)
}

// Swap is part of sort.Interface.
func (o *OrderBy) Swap(i, j int) {
	o.tuples[i], o.tuples[j] = o.tuples[j], o.tuples[i]
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (o *OrderBy) Less(i, j int) bool {
	p, q := o.tuples[i], o.tuples[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(o.orderBy); k++ {
		pValue, err := o.orderBy[k].EvalExpr(p)
		if err != nil {
			DebugOrder("Got error while sorting %v", err)
		}
		qValue, err := o.orderBy[k].EvalExpr(q)
		// DebugOrder("vals are %v \n %v \n %v %v %v %v \n\n\n", *p, *q, pValue, qValue, pValue.EvalPred(qValue, OpLt), pValue.EvalPred(qValue, OpGt))
		if err != nil {
			DebugOrder("Got error while sorting %v", err)
		}

		if pValue.EvalPred(qValue, OpLt) {
			// p < q, so we have a decision.
			return o.ascending[k]
		}

		if pValue.EvalPred(qValue, OpGt) {
			// p > q, so we have a decision.
			return !o.ascending[k]
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just output true
	return false
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	DebugOrder("in order iterator")
	childIter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	var tupleList []*Tuple
	collectedTuples := false
	i := -1
	return func() (*Tuple, error) {
		// for when we first process, loop back and start returning tups
		for {
			// we've already sorted everything, return tup
			if collectedTuples {
				i++
				if i >= len(tupleList) {
					return nil, nil
				}
				return tupleList[i], nil
			}

			// iterates thru all child tuples, collect them all
			for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
				if err != nil {
					return nil, err
				}

				// done going through the tuples
				if t == nil {
					collectedTuples = true
					break
				}
				tupleList = append(tupleList, t)
			}
			collectedTuples = true

			// collected all the tups, now sort them
			DebugOrder("Sorting%v %v", o.orderBy, o.ascending)
			// for _, tup := range tupleList {
			// 	DebugOrder("%v", *tup)
			// }
			o.Sort(tupleList)
			DebugOrder("Done sorting")
			// for _, tup := range tupleList {
			// 	DebugOrder("%v", *tup)
			// }
		}
	}, nil
}
