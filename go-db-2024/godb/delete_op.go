package godb

import "fmt"

type DeleteOp struct {
	// TODO: some code goes here
	deleteFile DBFile
	child      Operator
}

var DEBUGDELETEOP = false

func DebugDeleteOp(format string, a ...any) (int, error) {
	if DEBUGDELETEOP || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	return &DeleteOp{deleteFile, child}
}

func (dop *DeleteOp) Statistics() map[string]map[string]float64 {
	return dop.child.Statistics()
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}

}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	DebugDeleteOp("Got here 7")
	childIter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}
	DebugDeleteOp("Got here 8")

	return func() (*Tuple, error) {
		deletedTups := 0
		DebugDeleteOp("Got here 10")
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			DebugDeleteOp("Got here in child iter")

			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}

			err := dop.deleteFile.deleteTuple(t, tid)
			if err != nil {
				return nil, err
			}
			deletedTups++
		}
		DebugDeleteOp("Got here returning %v", deletedTups)

		return &Tuple{*dop.Descriptor(), []DBValue{IntField{int64(deletedTups)}}, nil}, nil
	}, nil
}
