package godb

import "fmt"

var DEBUGJOIN = false

func DebugJoin(format string, a ...any) (int, error) {
	if DEBUGJOIN || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
}

// Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [TupleDesc.merge].
func (hj *EqualityJoin) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())
}

// Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here

	// ------ We're gonna do a blocked hash join for SPEED -------
	// well, not really a hash, we just store a map of every value from our outer block r.

	var rightIter func() (*Tuple, error)
	rightIter = nil

	var leftBufferFloat map[float64][]*Tuple
	var leftBufferInt map[int64][]*Tuple
	var leftBufferString map[string][]*Tuple

	// get the type of field we're joining on
	intValue := false
	stringValue := false
	floatValue := false
	switch joinOp.leftField.GetExprType().Ftype {
	case StringType:
		stringValue = true
	case IntType:
		intValue = true
	case FloatType:
		floatValue = true
	}
	DebugJoin("left intValue is %v and stringValue is %v floatValue is %v left %v right %v\n", intValue, stringValue, floatValue, joinOp.leftField.GetExprType(), joinOp.rightField.GetExprType())

	switch joinOp.rightField.GetExprType().Ftype {
	case StringType:
		stringValue = true
	case IntType:
		DebugJoin("got an int type? %v\n", joinOp.rightField.GetExprType())
		intValue = true
	case FloatType:
		floatValue = true
	}

	DebugJoin("right intValue is %v and stringValue is %v floatValue is %v left %v right %v\n", intValue, stringValue, floatValue, joinOp.leftField.GetExprType(), joinOp.rightField.GetExprType())

	// make sure we don't have type mismatch
	if (intValue && stringValue) || (stringValue && intValue) ||
		(floatValue && stringValue) || (stringValue && floatValue) {
		return nil, GoDBError{TypeMismatchError, fmt.Sprintf("intValue is %v and stringValue is %v floatValue is %v left %v right %v", intValue, stringValue, floatValue, joinOp.leftField.GetExprType(), joinOp.rightField.GetExprType())}
	}

	// grab iterator through left outer table
	leftIter, err := (*joinOp.left).Iterator(tid)
	if err != nil {
		// DebugJoin("got err getting left iter: %v", err)
		return nil, err
	}

	// this will keep track of if we're processing the matches in the left map buffer for given right val
	processingHashList := false
	currentListIdx := 0
	var currentLeftTupleList []*Tuple
	var currentRightTuple *Tuple

	// the function iterator
	getTuple := func() (*Tuple, error) {
		for {

			// join is done, nothing to return
			if leftIter == nil && rightIter == nil {
				DebugJoin("left iter nil, returning nil")
				return nil, nil
			}

			// see if we're still processing a match from our left tuple buffer
			if processingHashList {
				if len(currentLeftTupleList) <= currentListIdx+1 {
					DebugJoin("stopping processing match, current idx is %v", currentListIdx)
					processingHashList = false
				} else {
					DebugJoin("still processing matching, current idx is currentListIdx")
					currentListIdx++
					return joinTuples(currentLeftTupleList[currentListIdx], currentRightTuple), nil
				}
			}

			// if we're through the right list, refresh right iter and left buffer
			if rightIter == nil {
				rightIter, err = (*joinOp.right).Iterator(tid)
				if err != nil {
					DebugJoin("got err getting right iter: %v", err)
					return nil, err
				}

				// refresh the buffer for next joinOp.maxBufferSize records
				// EXTREMELY ANNOYING TYPE CHECKING. warts of go :(
				if intValue {
					leftBufferInt = make(map[int64][]*Tuple)
					// read through the left iterator until our buffer is full
					for len(leftBufferInt) < joinOp.maxBufferSize && leftIter != nil {
						tup, err := leftIter()
						if err != nil {
							DebugJoin("Got err getting tuple: %v", err)
							return nil, err
						}

						// EOF of left outer table
						if tup == nil {
							leftIter = nil
							continue
						}

						DebugJoin("processing tuple left: %v", *tup)

						leftValue, err := joinOp.leftField.EvalExpr(tup)
						if err != nil {
							DebugJoin("got err getting left tup: %v", err)
							return nil, err
						}

						// add value to our left buffer map
						switch leftIntVal := leftValue.(type) {
						case IntField:
							_, ok := leftBufferInt[leftIntVal.Value]
							if !ok {
								leftBufferInt[leftIntVal.Value] = make([]*Tuple, 0)
							}

							leftBufferInt[leftIntVal.Value] = append(leftBufferInt[leftIntVal.Value], tup)
						case FloatField:
							return nil, GoDBError{TypeMismatchError, fmt.Sprintf("Should never get here 1: intvalue: %v, floatValue: %v, stringValue: %v", intValue, floatValue, stringValue)}
						case StringField:
							return nil, GoDBError{TypeMismatchError, "Should never get here 2"}
						}
					}
				} else if floatValue {
					leftBufferFloat = make(map[float64][]*Tuple)
					// read through the left iterator until our buffer is full
					for len(leftBufferFloat) < joinOp.maxBufferSize && leftIter != nil {
						tup, err := leftIter()
						if err != nil {
							DebugJoin("Got err getting tuple: %v", err)
							return nil, err
						}

						// EOF of left outer table
						if tup == nil {
							leftIter = nil
							continue
						}

						DebugJoin("processing tuple left: %v", *tup)

						leftValue, err := joinOp.leftField.EvalExpr(tup)
						if err != nil {
							DebugJoin("got err getting left tup: %v", err)
							return nil, err
						}

						// add value to our left buffer map
						switch leftFloatValue := leftValue.(type) {
						case FloatField:
							_, ok := leftBufferFloat[leftFloatValue.Value]
							if !ok {
								leftBufferFloat[leftFloatValue.Value] = make([]*Tuple, 0)
							}

							leftBufferFloat[leftFloatValue.Value] = append(leftBufferFloat[leftFloatValue.Value], tup)
						case IntField:
							return nil, GoDBError{TypeMismatchError, "Should never get here 3"}
						case StringField:
							return nil, GoDBError{TypeMismatchError, fmt.Sprintf("should never get here 4, %v %v %v %v", leftValue, floatValue, intValue, stringValue)}
						}
					}
				} else {
					leftBufferString = make(map[string][]*Tuple)
					for len(leftBufferString) < joinOp.maxBufferSize && leftIter != nil {
						tup, err := leftIter()
						if err != nil {
							DebugJoin("Got err getting tuple: %v", err)
							return nil, err
						}

						// EOF of left outer table
						if tup == nil {
							leftIter = nil
							continue
						}

						DebugJoin("processing tuple left: %v", *tup)

						leftValue, err := joinOp.leftField.EvalExpr(tup)
						if err != nil {
							DebugJoin("got err getting left tup: %v", err)
							return nil, err
						}

						// add value to our left buffer map
						switch leftStringVal := leftValue.(type) {
						case IntField:
							return nil, GoDBError{TypeMismatchError, "Should never get here 5"}
						case FloatField:
							return nil, GoDBError{TypeMismatchError, "Should never get here 6"}

						case StringField:
							_, ok := leftBufferString[leftStringVal.Value]
							if !ok {
								leftBufferString[leftStringVal.Value] = make([]*Tuple, 0)
							}

							leftBufferString[leftStringVal.Value] = append(leftBufferString[leftStringVal.Value], tup)
						}
					}
				}
			}

			if leftIter == nil {
				DebugJoin("left iter nil but still processing value, buffer size is %v %v", len(leftBufferInt), len(leftBufferString))
			}

			tup, err := rightIter()
			if err != nil {
				DebugJoin("Got err getting tuple: %v", err)
				return nil, err
			}

			// EOF for right table, get next block from left
			if tup == nil {
				rightIter = nil
				continue
			}

			DebugJoin("processing tuple right: %v", *tup)

			// check if tuple value in our buffer map
			rightValue, err := joinOp.rightField.EvalExpr(tup)
			if err != nil {
				DebugJoin("got err evaluating right field: %v", err)
				return nil, err
			}

			switch rightVal := rightValue.(type) {
			case IntField:
				matchingTuples, ok := leftBufferInt[rightVal.Value]
				if !ok {
					break
				}
				if len(matchingTuples) == 0 {
					return nil, GoDBError{TypeMismatchError, fmt.Sprintf("Got length 0 match for %v, shouldn't happen", rightVal.Value)}
				}
				DebugJoin("got hit, len matching tuples is %v", len(matchingTuples))
				processingHashList = true
				currentListIdx = 0
				currentLeftTupleList = matchingTuples
				currentRightTuple = tup
				return joinTuples(matchingTuples[0], tup), nil
			case FloatField:
				matchingTuples, ok := leftBufferFloat[rightVal.Value]
				if !ok {
					break
				}
				if len(matchingTuples) == 0 {
					return nil, GoDBError{TypeMismatchError, fmt.Sprintf("Got length 0 match for %v, shouldn't happen", rightVal.Value)}
				}
				DebugJoin("got hit, len matching tuples is %v", len(matchingTuples))
				processingHashList = true
				currentListIdx = 0
				currentLeftTupleList = matchingTuples
				currentRightTuple = tup
				return joinTuples(matchingTuples[0], tup), nil
			case StringField:
				matchingTuples, ok := leftBufferString[rightVal.Value]
				if !ok {
					break
				}
				if len(matchingTuples) == 0 {
					return nil, GoDBError{TypeMismatchError, fmt.Sprintf("Got length 0 match for %v, shouldn't happen", rightVal.Value)}
				}
				DebugJoin("got hit, len matching tuples is %v", len(matchingTuples))
				processingHashList = true
				currentListIdx = 0
				currentLeftTupleList = matchingTuples
				currentRightTuple = tup
				return joinTuples(matchingTuples[0], tup), nil
			}
		}
	}

	return getTuple, nil
}
