package godb

import (
	"fmt"
	"math/rand"
	"time"
)

//Expressions can be applied to tuples to get concrete values.  They
//encapsulate constates, simple fields, and functions over multiple other
//expressions.  We have provided the expression methods for you;  you will need
//use the [EvalExpr] method  in your operator implementations to get fields and
//other values from tuples.

type Expr interface {
	EvalExpr(t *Tuple) (DBValue, error) //DBValue is either IntField or StringField
	GetExprType() FieldType             //Return the type of the Expression
}

type FieldExpr struct {
	selectField FieldType
}

func (f *FieldExpr) EvalExpr(t *Tuple) (DBValue, error) {
	outTup, err := t.project([]FieldType{f.selectField})
	if err != nil {
		//fmt.Printf("err in project: %s", err.Error())
		return nil, err
	}
	return outTup.Fields[0], nil

}

func (f *FieldExpr) GetExprType() FieldType {
	return f.selectField
}

type ConstExpr struct {
	val       DBValue
	constType DBType
}

func (c *ConstExpr) GetExprType() FieldType {
	return FieldType{"const", fmt.Sprintf("%v", c.val), c.constType}
}

func (c *ConstExpr) EvalExpr(_ *Tuple) (DBValue, error) {
	return c.val, nil
}

type FuncExpr struct {
	op   string
	args []*Expr
}

func (f *FuncExpr) GetExprType() FieldType {
	fType, exists := funcs[f.op]
	//todo return err
	if !exists {
		return FieldType{f.op, "", IntType}
	}
	ft := FieldType{f.op, "", IntType}
	for _, fe := range f.args {
		fieldExpr, ok := (*fe).(*FieldExpr)
		if ok {
			ft = fieldExpr.GetExprType()
		}
	}
	return FieldType{ft.Fname, ft.TableQualifier, fType.outType}

}

type FuncType struct {
	argTypes []DBType
	outType  DBType
	f        func([]any) any
}

var overloadedFuncs = map[string][]FuncType{
	"+":    {{[]DBType{FloatType, FloatType}, FloatType, addFuncFloats}, {[]DBType{IntType, IntType}, IntType, addFuncInts}, {[]DBType{FloatType, IntType}, FloatType, addFuncFloats}, {[]DBType{IntType, FloatType}, FloatType, addFuncFloats}},
	"-":    {{[]DBType{FloatType, FloatType}, FloatType, minusFuncFloats}, {[]DBType{IntType, IntType}, IntType, minusFuncInts}, {[]DBType{FloatType, IntType}, FloatType, minusFuncFloats}, {[]DBType{IntType, FloatType}, FloatType, minusFuncFloats}},
	"*":    {{[]DBType{FloatType, FloatType}, FloatType, timesFuncFloats}, {[]DBType{IntType, IntType}, IntType, timesFuncInts}, {[]DBType{FloatType, IntType}, FloatType, timesFuncFloats}, {[]DBType{IntType, FloatType}, FloatType, timesFuncFloats}},
	"/":    {{[]DBType{FloatType, FloatType}, FloatType, divFuncFloats}, {[]DBType{IntType, IntType}, IntType, divFuncInts}, {[]DBType{FloatType, IntType}, FloatType, divFuncFloats}, {[]DBType{IntType, FloatType}, FloatType, divFuncFloats}},
	"sq":   {{[]DBType{FloatType}, FloatType, sqFuncFloat}, {[]DBType{IntType}, IntType, sqFuncInt}},
	"nmin": {{[]DBType{FloatType, FloatType}, FloatType, minFuncFloats}, {[]DBType{IntType, IntType}, IntType, minFuncInts}, {[]DBType{FloatType, IntType}, FloatType, minFuncFloats}, {[]DBType{IntType, FloatType}, FloatType, minFuncFloats}},
	"nmax": {{[]DBType{FloatType, FloatType}, FloatType, maxFuncFloats}, {[]DBType{IntType, IntType}, IntType, maxFuncInts}, {[]DBType{FloatType, IntType}, FloatType, maxFuncFloats}, {[]DBType{IntType, FloatType}, FloatType, maxFuncFloats}},
}

var funcs = map[string]FuncType{
	//note should all be lower case
	// "+":                     {[]DBType{FloatType, FloatType}, FloatType, addFunc},
	// "-":                     {[]DBType{FloatType, FloatType}, FloatType, minusFunc},
	// "*":                     {[]DBType{FloatType, FloatType}, FloatType, timesFunc},
	// "/":                     {[]DBType{FloatType, FloatType}, FloatType, divFunc},
	"mod":       {[]DBType{IntType, IntType}, IntType, modFunc},
	"randInt":   {[]DBType{}, IntType, randIntFunc},
	"randFloat": {[]DBType{FloatType, FloatType}, FloatType, randFloatFunc},
	// "sq":                    {[]DBType{FloatType}, FloatType, sqFunc},
	"getsubstr":             {[]DBType{StringType, IntType, IntType}, StringType, subStrFunc},
	"epoch":                 {[]DBType{}, IntType, epoch},
	"datetimestringtoepoch": {[]DBType{StringType}, IntType, dateTimeToEpoch},
	"datestringtoepoch":     {[]DBType{StringType}, IntType, dateToEpoch},
	"epochtodatetimestring": {[]DBType{IntType}, StringType, dateString},
	"imin":                  {[]DBType{IntType, IntType}, IntType, minFuncInts},
	"imax":                  {[]DBType{IntType, IntType}, IntType, maxFuncInts},
	"fmin":                  {[]DBType{FloatType, FloatType}, FloatType, minFuncFloats},
	"fmax":                  {[]DBType{FloatType, FloatType}, FloatType, minFuncFloats},
}

func ListOfFunctions() string {
	fList := ""
	processFunc := func(name string, f FuncType) {
		args := "("
		argList := f.argTypes
		hasArg := false
		for _, a := range argList {
			if hasArg {
				args = args + ","
			}
			switch a {
			case IntType:
				args = args + "int"
			case FloatType:
				args = args + "float"
			case StringType:
				args = args + "string"
			}
			hasArg = true
		}
		args = args + ")"
		fList = fList + "\t" + name + args + "\n"
	}
	for name, funcList := range overloadedFuncs {
		for _, f := range funcList {
			processFunc(name, f)
		}
	}
	for name, f := range funcs {
		processFunc(name, f)
	}
	return fList
}
func minFuncInts(args []any) any {
	first := args[0].(int64)
	second := args[1].(int64)
	if first < second {
		return first
	}
	return second
}

func minFuncFloats(args []any) any {
	first := args[0].(float64)
	second := args[1].(float64)
	if first < second {
		return first
	}
	return second
}

func maxFuncInts(args []any) any {
	first := args[0].(int64)
	second := args[1].(int64)
	if first >= second {
		return first
	}
	return second
}

func maxFuncFloats(args []any) any {
	first := args[0].(float64)
	second := args[1].(float64)
	if first >= second {
		return first
	}
	return second
}

func dateTimeToEpoch(args []any) any {
	inString := args[0].(string)
	tt, err := time.Parse(time.UnixDate, inString)
	if err != nil {
		return int64(0)
	}
	return int64(time.Time.Unix(tt))
}

func dateToEpoch(args []any) any {
	inString := args[0].(string)
	tt, err := time.Parse("2006-01-02", inString)
	if err != nil {
		return int64(0)
	}
	return int64(time.Time.Unix(tt))
}

func dateString(args []any) any {
	unixTime := args[0].(int64)
	t := time.Unix(unixTime, 0)
	strDate := t.Format(time.UnixDate)
	return strDate
}

func epoch(args []any) any {
	t := time.Now()
	return time.Time.Unix(t)
}

func randIntFunc(args []any) any {
	return int64(rand.Int())
}

func randFloatFunc(args []any) any {
	return args[0].(float64) + rand.Float64()*(args[1].(float64)-args[0].(float64))
}

func modFunc(args []any) any {
	return args[0].(int64) % args[1].(int64)
}

func divFuncInts(args []any) any {
	return args[0].(int64) / args[1].(int64)
}

func divFuncFloats(args []any) any {
	return args[0].(float64) / args[1].(float64)
}

func timesFuncInts(args []any) any {
	return args[0].(int64) * args[1].(int64)
}

func timesFuncFloats(args []any) any {
	return args[0].(float64) * args[1].(float64)
}

func minusFuncInts(args []any) any {
	return args[0].(int64) - args[1].(int64)
}

func minusFuncFloats(args []any) any {
	return args[0].(float64) - args[1].(float64)
}

func addFuncInts(args []any) any {
	return args[0].(int64) + args[1].(int64)
}

func addFuncFloats(args []any) any {
	return args[0].(float64) + args[1].(float64)
}

func sqFuncInt(args []any) any {
	return args[0].(int64) * args[0].(int64)
}

func sqFuncFloat(args []any) any {
	return args[0].(float64) * args[0].(float64)
}

func subStrFunc(args []any) any {
	stringVal := args[0].(string)
	start := args[1].(int64)
	numChars := args[2].(int64)

	var substr string
	if start < 0 || start > int64(len(stringVal)) {
		substr = ""
	} else if start+numChars > int64(len(stringVal)) {
		substr = stringVal[start:]
	} else {
		substr = stringVal[start : start+numChars]
	}

	return substr
}

func (f *FuncExpr) EvalExpr(t *Tuple) (DBValue, error) {
	processFunc := func(fType FuncType) (DBValue, error) {
		if len(f.args) != len(fType.argTypes) {
			return nil, GoDBError{ParseError, fmt.Sprintf("function %s expected %d args", f.op, len(fType.argTypes))}
		}
		argvals := make([]any, len(fType.argTypes))
		for i, argType := range fType.argTypes {
			arg := *f.args[i]
			if arg.GetExprType().Ftype != argType {
				typeName := "string"
				switch argType {
				case IntType:
					typeName = "int"
				case FloatType:
					typeName = "float"
				}
				return nil, GoDBError{ParseError, fmt.Sprintf("function %s expected arg of type %s, got %v expected %v", f.op, typeName, arg.GetExprType().Ftype, argType)}
			}
			val, err := arg.EvalExpr(t)
			if err != nil {
				return nil, err
			}
			switch argType {
			case IntType:
				argvals[i] = val.(IntField).Value
			case FloatType:
				argvals[i] = val.(FloatField).Value
			case StringType:
				argvals[i] = val.(StringField).Value
			}
		}
		result := fType.f(argvals)
		switch fType.outType {
		case IntType:
			return IntField{result.(int64)}, nil
		case FloatType:
			return FloatField{result.(float64)}, nil
		case StringType:
			return StringField{result.(string)}, nil
		}
		return nil, GoDBError{ParseError, "unknown result type in function"}
	}
	funcList, exists := overloadedFuncs[f.op]
	if exists {
		for _, funcType := range funcList {
			val, err := processFunc(funcType)
			if err == nil {
				return val, err
			}
		}
		args := make([]FieldType, len(f.args))
		for i, e := range f.args {
			args[i] = (*e).GetExprType()
		}
		return nil, GoDBError{ParseError, fmt.Sprintf("unknown function %v %v", f, args)}
	}
	fType, exists := funcs[f.op]
	if !exists {
		return nil, GoDBError{ParseError, fmt.Sprintf("unknown function %s", f.op)}
	}
	return processFunc(fType)
}
