package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Define these here instead of types.go cause it seems autograder overwrites types.go with base version
const (
	// PageSize     int = 4096
	HeaderSize int = 8
	// StringLength int = 32
	Int64Length          int           = 8
	RepInvariantViolated GoDBErrorCode = 13
)

var DEBUGTUPLE = false
var GLOBALDEBUG = false

func DebugTuple(format string, a ...any) (int, error) {
	if DEBUGTUPLE || GLOBALDEBUG {
		return fmt.Println(fmt.Sprintf(format, a...))
	}
	return 0, nil
}

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

func (t DBType) String() string {
	switch t {
	case IntType:
		return "int"
	case StringType:
		return "string"
	}
	return "unknown"
}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	// TODO: some code goes here
	if d1 == nil && d2 == nil {
		return true
	}

	if d1 == nil || d2 == nil {
		return false
	}

	if len(d1.Fields) != len(d2.Fields) {
		return false
	}
	for i, field := range d1.Fields {
		if field != d2.Fields[i] {
			return false
		}
	}
	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	if td == nil {
		return nil
	}
	copiedFields := make([]FieldType, len(td.Fields))
	copy(copiedFields, td.Fields)
	return &TupleDesc{
		Fields: copiedFields,
	}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO: some code goes here
	if desc == nil && desc2 == nil {
		return nil
	}

	if desc2 == nil {
		return desc.copy()
	}

	if desc == nil {
		return desc2.copy()
	}

	mergedFields := make([]FieldType, len(desc.Fields)+len(desc2.Fields))
	copy(mergedFields, desc.Fields)
	copy(mergedFields[len(desc.Fields):], desc2.Fields)
	return &TupleDesc{
		Fields: mergedFields,
	}
}

// ================== Tuple Methods ======================

// Interface for tuple field values
type DBValue interface {
	EvalPred(v DBValue, op BoolOp) bool
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

type recordIDImpl struct {
	pageNo int
	slotNo int
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO: some code goes here

	if t == nil {
		return nil
	}

	var err error
	for i, dbValue := range t.Fields {
		switch fieldType := dbValue.(type) {
		case StringField:
			// make sure it's a string in the desc
			if descType := t.Desc.Fields[i].Ftype; descType != StringType {
				return GoDBError{TypeMismatchError, fmt.Sprintf("Should be string type here %v", descType)}
			}

			if len(fieldType.Value) > StringLength {
				return GoDBError{MalformedDataError, fmt.Sprintf("String length %v > StringLength %v", len(fieldType.Value), StringLength)}
			}

			// pad it as necessary
			byteString := make([]byte, StringLength)
			copy(byteString, []byte(fieldType.Value))

			binary.Write(b, binary.LittleEndian, byteString)
		case IntField:
			// make sure it's an int in the desc
			if descType := t.Desc.Fields[i].Ftype; descType != IntType {
				return GoDBError{TypeMismatchError, fmt.Sprintf("Should be int type here %v", descType)}
			}
			binary.Write(b, binary.LittleEndian, fieldType.Value)
		}
	}

	return err
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficient data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	if desc == nil {
		return nil, GoDBError{MalformedDataError, "Got a nil desc!!"}
	}

	tupleFields := make([]DBValue, len(desc.Fields))
	tuple := Tuple{
		Desc:   *desc,
		Fields: tupleFields,
		Rid:    nil,
	}

	var err error
	for i, fieldType := range desc.Fields {
		switch fieldType.Ftype {
		case StringType:

			// get the padded string
			byteString := make([]byte, StringLength)
			err = binary.Read(b, binary.LittleEndian, &byteString)
			if err != nil {
				return &tuple, err
			}

			// only use the non padded string
			tuple.Fields[i] = StringField{Value: string(bytes.TrimRight(byteString, "\x00"))}
		case IntType:

			var intValue int64
			err = binary.Read(b, binary.LittleEndian, &intValue)
			if err != nil {
				return &tuple, err
			}

			tuple.Fields[i] = IntField{Value: intValue}
		}
	}

	return &tuple, nil
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	if t1 == nil && t2 == nil {
		return true
	}

	if t1 == nil || t2 == nil {
		return false
	}

	if !t1.Desc.equals(&t2.Desc) {
		return false
	}

	for i, dbValue := range t1.Fields {
		if dbValue != t2.Fields[i] {
			return false
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2
// appended to t1. The new tuple should have a correct TupleDesc that is created
// by merging the descriptions of the two input tuples.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here

	if t1 == nil && t2 == nil {
		return nil
	}

	if t1 == nil {
		// make a copy of t2
		newFields := make([]DBValue, len(t2.Fields))
		copy(newFields, t2.Fields)
		return &Tuple{
			Desc:   *t2.Desc.copy(),
			Fields: newFields,
		}
	}

	if t2 == nil {
		// make a copy of t1
		newFields := make([]DBValue, len(t1.Fields))
		copy(newFields, t1.Fields)
		return &Tuple{
			Desc:   *t1.Desc.copy(),
			Fields: newFields,
		}
	}

	numFields := len(t1.Fields) + len(t2.Fields)
	combinedFields := make([]DBValue, numFields)
	copy(combinedFields, t1.Fields)
	copy(combinedFields[len(t1.Fields):], t2.Fields)

	return &Tuple{
		Desc:   *t1.Desc.merge(&t2.Desc),
		Fields: combinedFields,
	}
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
//
// Note that EvalExpr uses the [Tuple.project] method, so you will need
// to implement projection before testing compareField.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here

	if t == nil && t2 == nil {
		return OrderedEqual, nil
	}

	t1Result, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err
	}
	t2Result, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}

	t1FieldType := reflect.TypeOf(t1Result)
	t2FieldType := reflect.TypeOf(t2Result)

	if t1FieldType != t2FieldType {
		return OrderedEqual, GoDBError{TypeMismatchError, fmt.Sprintf("Incompatible types for comparison: %v and %v", t1FieldType, t2FieldType)}
	}

	switch t1Type := t1Result.(type) {
	case StringField:
		t2Type, ok := t2Result.(StringField)
		if !ok {
			return OrderedEqual, GoDBError{TypeMismatchError, fmt.Sprintf("Should never happen, expected string field, got %v and %v", t1FieldType, t2FieldType)}
		}
		if t1Type.Value > t2Type.Value {
			return OrderedGreaterThan, nil
		}
		if t1Type.Value < t2Type.Value {
			return OrderedLessThan, nil
		}
		return OrderedEqual, nil
	case IntField:
		t2Type, ok := t2Result.(IntField)
		if !ok {
			return OrderedEqual, GoDBError{TypeMismatchError, fmt.Sprintf("Should never happen, expected int field, got %v and %v", t1FieldType, t2FieldType)}
		}
		if t1Type.Value > t2Type.Value {
			return OrderedGreaterThan, nil
		}
		if t1Type.Value < t2Type.Value {
			return OrderedLessThan, nil
		}
		return OrderedEqual, nil
	default:
		return OrderedEqual, GoDBError{TypeMismatchError, fmt.Sprintf("Unsupported types found, got %v and %v", t1FieldType, t2FieldType)}
	}
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here

	if t == nil {
		return nil, nil
	}

	dbValues := make([]DBValue, len(fields))
	descFields := make([]FieldType, len(fields))

	for i, fieldType := range fields {
		idx, err := findFieldInTd(fieldType, &t.Desc)
		if err != nil {
			return nil, err
		}

		dbValues[i] = t.Fields[idx]
		descFields[i] = t.Desc.Fields[idx]
	}

	return &Tuple{
		Desc: TupleDesc{
			Fields: descFields,
		},
		Fields: dbValues,
	}, nil

}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {
	var buf bytes.Buffer
	t.writeTo(&buf)
	return buf.String()
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = strconv.FormatInt(f.Value, 10)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr
}