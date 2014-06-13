rel
========

Relational Algebra in Go.  Most Go doesn't need relational algebra, but most relational algebra needs Go.  The inherent parallelism in relational operations is buried by RDBMS's, and is much less extensible in function.  The cleanness of expression is missing in most ORMs, which prevents most query optimization.  Go provides the tools which can take advantage of both that parallelism and query optimization.  It is my hope that this package will prove that is true.

This implements most of the traditional elements of relational algebra, including project, restrict, join, setdiff, and union.  It also implements some of the common non-relational operations, including groupby, map, and order.  To learn more about relational algebra, C. J. Date's Database in Depth is a great place to start, and is used as the source of terminology in the rel package.

The semantics of this package are very similar to Microsoft's LINQ, although the syntax is somewhat different.  This isn't LINQ though - it is a library, and it is not integrated with the language, which means rel has a significant performance cost relative to normal go code that doesn't use reflection.  Unlike LINQ, you can see how it does everything, and have more control over how queries are executed.

Interfaces
==========
The uses of the interfaces defined in the rel package are outlined here.

Relation Interface
------------------

Relations are channels of tuples, and operations on those channels.  The relational algebra operations of project, restrict, join, intersect, setdiff, and union all take at least one relation input and result in a relation output.  Many of the relational operations
feature query rewrite, using the rules of relational algebra, for example: http://www.dcs.warwick.ac.uk/~wmb/CS319/pdf/opt.pdf.

Results tuples can be cancelled, or Relational operations can be an error.  The difference between them is that errors come from the source, and cancellation comes from the sink.

Predicate Interface
-------------------
Predicates are used in the restrict operation.


TODOs
=====
+ Add errors to indicate when relations are constructed from invalid operations
+ finish coverage on error short circuit
+ Use the go race detector & clear up any issues.  (this requires a 64bit arch)
+ Reach 100% test coverage (currently 85%)
+ Implement tests with deterministic output of relational operations.  Currently tests for things like GoString, join, and groupby are dependent on the (arbitrary) order of output tuples.  They should go through an orderby operation first, or just compare against a known good relation through setdiff.
+ Implement sub packages for other data sources, such as csv readers, generic sql tables, json, or gob.
+ Implement non relational operations like order.
+ Write better docs
+ Write single godoc file (possible examples include matrix algebra & creating a new relation type)
+ Hook up chan_mem to some kind of copying mechanism.
+ Add more predicate tests
+ Add candidate key tests

Errors
======
There are 2 types of errors that can be handled: errors in relational construction, like projecting a relation to a set of tuples that are not a subset of the original relation, and errors during computation, like when a data source unexpectedly disconnects.  There are two types of error handling available to us: either panic (and maybe recover) which is expensive, or having some kind of Error() method of relations, which returns an error.  If no error has been encountered, then Error should return nil, otherwise some kind of formatted string.  Having 2-arg outputs is not conducive to the method chaining that is used.

I think the best course of action is to reserve panic for problems that are not possible for the rel package to handle in advance - like a type error in an AdHoc predicate or grouping function, which rel has no control over.

To that end, rel will go the Error() route, which will be checked in the following places:

1) during derived relational construction, if one of the source relations is an error, then that relation will be returned instead of the compound relation.  In the case that two relations are provided and both are errors, then the first will be returned.
2) in the tuples method, if the source(s) of tuples are closed, then they are checked for an error.  If it is not nil, then the error is set in the derived relation, and the results channel is closed.
3) in the tuples method, if the relation already has non-nil error, then the results channel is immediately closed.

Draft Golang Nuts Announcement
==============================
[ANN] Relational Algebra

rel is a relational algebra package for Go, available at https://github.com/jonlawlor/rel.  It provides an extensible ORM which can perform query rewrite.  Relations are implemented as pipelines of tuples that are transformed and composed to produce results through a channel.  The package is currently experimental, interfaces are subject to change, and you should not use it for anything requiring high performance.

Relational queries are ideally suited to parallel execution; they consist of uniform operations applied to uniform streams of data. Each operator produces a new relation, so the operators can be composed into highly parallel dataflow graphs. By streaming the output of one operator into the input of another operator, the two operators can work in series giving pipelined parallelism. By partitioning the input data among multiple processors and memories, an operator can often be split into many independent operators each working on a part of the data. This partitioned data and execution gives partitioned parallelism. [1]

Go's concurrency is great at expressing that pipelined parallelism.  Most relational databases & ORMs completely hide those details.

Go's interfaces are great at providing succinct extensibility.  Most relational databases do not allow much in the way of extensibility; many do not allow arbitrary types of attributes, and very few allow new relational expressions to be defined.

We believe that the combination of relations, concurrency, and extensibility allows for something unique and interesting.  This package is made to explore that idea.

Here's an example of sparse matrix multiplication using rel:

```go
type matrixElem struct {
	R int
	C int
	V float64
}
type multElemA struct {
	R int
	M int
	VA float64
}	
type multElemB struct {
	M int
	C int
	VB float64
}
type multElemC struct {
	R int
	C int
	M int
	VA float64
	VB float64
}
type multRes struct {
	R int
	C int
	M int
	V float64
}
mapMult := func (tup rel.T) rel.T {
	if v, ok := tup.(multElemC); ok {
		return multRes{v.R, v.C, v.M, v.VA * v.VB}
	} else {
		return multRes{}
	}
}
type valTup struct {
	V float64
}
groupAdd := func(val <-chan rel.T) rel.T {
	res := valTup{}
	for vi := range val {
		v := vi.(valTup)
		res.V += v.V
	}
	return res
}

// representation of a matrix:
//  1 2 
//  3 4 
A := rel.New([]matrixElem{
	{1, 1, 1.0},
	{1, 2, 2.0},
	{2, 1, 3.0},
	{2, 2, 4.0},
},[][]string{[]string{"R", "C"}})

// representation of a matrix:
//  4 17 
//  9 17
B := New([]matrixElem{
	{1, 1, 4.0},
	{1, 2, 17.0},
	{2, 1, 9.0},
	{2, 2, 17.0},
},[][]string{[]string{"R", "C"}})


C := A.Rename(multElemA{}).Join(B.Rename(multElemB{}), multElemC{}).
	Map(mapMult, multRes{}, [][]string{[]string{"R", "C", "M"}}).
	GroupBy(matrixElem{}, valTup{}, groupAdd)
	
fmt.Println("\n%#v",C)

// Output:
// rel.New([]struct {
//  R int     
//  C int     
//  V float64 
// }{
//  {1, 1, 22,  },
//  {1, 2, 51,  },
//  {2, 1, 48,  },
//  {2, 2, 119, },
// })
```

Which isn't going to set any records for brevity or efficiency.  It also demonstrates some of the issues with its current state: it results in a lot of type definitions for intermediate tuple representation, which is an area we want to work on.  Behind the scenes there is quite a bit of reflection as well, which comes with other downsides, including slower performance and less type safety.

On the other hand, thanks to interface's duck typing, users of this package can define their own Relation(s), which we anticipate will be used similarly as LINQ's Type Providers.  The "literal" Relations above can be replaced by Relations that provide values from an SQL database, or a csv.Reader, which are defined in sub packages, or in a user defined source if they roll their own Relation, which can take advantage of the query rewrite rules in the rel package.

[1] Parallel Database Systems: The Future of Database Processing or a Passing Fad?

