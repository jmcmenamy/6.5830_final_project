package godb

import "fmt"

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	// You may want to add additional fields here
	distinct bool
	// TODO: some code goes here
}

var DEBUGPROJECT = false

func DebugProject(format string, a ...any) (int, error) {
	if DEBUGPROJECT || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	return &Project{selectFields, outputNames, child, distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	desc := &TupleDesc{Fields: make([]FieldType, len(p.selectFields))}
	for i, field := range p.selectFields {
		desc.Fields[i] = FieldType{p.outputNames[i], "", field.GetExprType().Ftype}
	}
	return desc
}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	seenTuples := make(map[any]bool)
	desc := p.Descriptor()
	return func() (*Tuple, error) {
		// wrap in for loop in case we're doing distinct
		for {
			childTup, err := childIter()

			if err != nil {
				return nil, err
			}

			// no more tups
			if childTup == nil {
				return nil, nil
			}

			DebugProject("child tuple is %v\n", childTup)
			projectedTup := &Tuple{*desc, make([]DBValue, len(desc.Fields)), nil}
			for i := 0; i < len(desc.Fields); i++ {
				dbValue, err := p.selectFields[i].EvalExpr(childTup)
				if err != nil {
					return nil, err
				}
				projectedTup.Fields[i] = dbValue
			}
			if p.distinct {
				projectedTupKey := projectedTup.tupleKey()
				_, ok := seenTuples[projectedTupKey]
				// seen tuple, iterate to the next
				if ok {
					continue
				}
				seenTuples[projectedTupKey] = true
			}
			return projectedTup, nil
		}
	}, nil
}
