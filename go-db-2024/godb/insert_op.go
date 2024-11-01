package godb

import "fmt"

type InsertOp struct {
	// TODO: some code goes here
	insertFile DBFile
	child      Operator
}

var DEBUGINSERTOP = false

func DebugInsertOp(format string, a ...any) (int, error) {
	if DEBUGINSERTOP || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	return &InsertOp{insertFile, child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	return func() (*Tuple, error) {
		insertedTups := 0
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}

			err := iop.insertFile.insertTuple(t, tid)
			if err != nil {
				return nil, err
			}
			insertedTups++
		}

		return &Tuple{*iop.Descriptor(), []DBValue{IntField{int64(insertedTups)}}, nil}, nil
	}, nil
}
