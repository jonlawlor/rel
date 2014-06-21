rel
===

Relational Algebra in Go.  Go's concurrency mechanisms allow for fine control of the inherent parallelism in relational operations.  Go's interfaces & duck typing are used to provide an extensible ORM that is capable of query rewrite. It is my hope that this package will produce some interesting approaches to implement relational expressions.  This package is currently experimental and its interfaces may change.

This implements most of the traditional elements of relational algebra, including project, restrict, join, setdiff, and union.  It also implements some of the common non-relational operations, including groupby, and map.  To learn more about relational algebra, C. J. Date's Database in Depth is a great place to start, and it is used as the source of terminology in the rel package.

Please note that relational algebra *_is not SQL_*.  In particular, NULL is not a part of relational algebra, and all relations must be distinct.

The semantics of this package are very similar to Microsoft's LINQ, although the syntax is somewhat different.  This isn't LINQ though - it is a library, and it is not integrated with the language, which means rel has a significant performance cost relative to normal go code that doesn't use reflection.

Interfaces
==========
The uses of the interfaces defined in the rel package are outlined here.

Relation Interface
------------------

Relations are channels of tuples, and operations on those channels.  The relational algebra operations of project, restrict, join, intersect, setdiff, and union all take at least one relation input and result in a relation output.  Many of the relational operations feature query rewrite, using the rules of relational algebra, for example: http://www.dcs.warwick.ac.uk/~wmb/CS319/pdf/opt.pdf.

Results tuples can be cancelled, or Relational operations can be an error.  The difference between them is that errors come from the source, and cancellation comes from the sink.

Predicate Interface
-------------------
Predicates are used in the restrict operation.


TODOs
=====
+ Add errors to indicate when relations are constructed from invalid operations
+ Reach 100% test coverage (currently 85%)
+ Implement sub packages for other data sources, such as generic sql tables, json, or gob.
+ Implement non relational operations like order.
+ Write better docs
+ Hook up chan_mem to some kind of copying mechanism.
+ Add more predicate tests
+ Should attributes have an associated type, or just a name like it is now?
+ Add candidate key tests

Errors
======
There are 2 types of errors that can be handled: errors in relational construction, like projecting a relation to a set of tuples that are not a subset of the original relation, and errors during computation, like when a data source unexpectedly disconnects.  There are two types of error handling available to us: either panic (and maybe recover) which is expensive, or having some kind of Err() method of relations, which returns an error.  If no error has been encountered, then Err should return nil, otherwise an error.  Having 2-arg outputs is not conducive to the method chaining that is used.  The Err() method way of handling errors is also used in the sql package's Scanner.

I think the best course of action is to reserve panic for problems that are not possible for the rel package to handle in advance - like a type error in an AdHoc predicate or grouping function, which rel has no control over.

To that end, rel will go the Error() route, which will be checked in the following places:

1) during derived relational construction, if one of the source relations is an error, then that relation will be returned instead of the compound relation.  In the case that two relations are provided and both are errors, then the first will be returned.
2) in the tuples method, if the source(s) of tuples are closed, then they are checked for an error.  If it is not nil, then the error is set in the derived relation, and the results channel is closed.
3) in the tuples method, if the relation already has non-nil error, then the results channel is immediately closed.

Cancellation
============
Cancellation is handled in the Tuples method.  If a caller no longer wants any results, they should close the cancel channel, which will then stop tuples from being sent by the Tuples method, which will also relay the cancellation up to any sources of tuples that it is consuming.  It will _not_ close the results channel.

Draft Golang Nuts Announcement
==============================
[ANN] Relational Algebra

rel is a relational algebra package for Go, available at https://github.com/jonlawlor/rel.  It provides an ORM which can perform extensible query rewrite.  Relations are implemented as pipelines of tuples that are transformed and composed to produce results through a channel.  The package is currently experimental, interfaces are subject to change, and you should not use it for anything requiring even medium performance.

quote:
Relational queries are ideally suited to parallel execution; they consist of uniform operations applied to uniform streams of data. Each operator produces a new relation, so the operators can be composed into highly parallel dataflow graphs. By streaming the output of one operator into the input of another operator, the two operators can work in series giving pipelined parallelism. By partitioning the input data among multiple processors and memories, an operator can often be split into many independent operators each working on a part of the data. This partitioned data and execution gives partitioned parallelism. [1]

It is my hope that this package will produce some interesting approaches to implement relational expressions.  Go's concurrency mechanisms allow for fine control of the inherent parallelism in relational operations.  Go's interfaces & duck typing are used to provide an extensible ORM that is capable of query rewrite. 

Here's an example of sparse matrix multiplication using rel (relational meets linear):

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
	R  int
	C  int
	M  int
	VA float64
	VB float64
}
type multRes struct {
	R int
	C int
	M int
	V float64
}
mapMult := func (tup interface{}) interface{} {
	if v, ok := tup.(multElemC); ok {
		return multRes{v.R, v.C, v.M, v.VA * v.VB}
	} else {
		return multRes{}
	}
}
type valTup struct {
	V float64
}
groupAdd := func(val <-chan interface{}) interface{} {
	res := valTup{}
	for vi := range val {
		v := vi.(valTup)
		res.V += v.V
	}
	return res
}

// representation of a matrix:
//  1.0 2.0
//  3.0 4.0 
A := rel.New([]matrixElem{
	{1, 1, 1.0},
	{1, 2, 2.0},
	{2, 1, 3.0},
	{2, 2, 4.0},
},[][]string{[]string{"R", "C"}})

// representation of a matrix:
//  4.0 17.0 
//  9.0 17.0
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

It isn't going to set any records for brevity or efficiency.  It demonstrates some of the issues with the rel package's current state: it results in a lot of type definitions for intermediate tuple representation, which is an area that requires work.  Behind the scenes there is quite a bit of reflection as well, which comes with other downsides, including slower performance and less type safety.  Also, every relational operation (except for no-ops) results in a new pipeline stage getting built, with more channels handing off values.

On the other hand, thanks to interface's duck typing, users of this package can define their own Relation(s), which could be used in a similar manner to LINQ's Type Providers.  The "literal" Relations above can be replaced by Relations that provide values from an SQL database, or a csv.Reader (https://github.com/jonlawlor/csv), or in a user defined source if they roll their own Relation, which can take advantage of the query rewrite rules in the rel package.

Why care about relational algebra over SQL?  Relational algebra is _so much simpler_ than the SQL standard.  The current SQL standard is hundreds of pages long and defines everything from regular expressions to recursive queries to interaction with Java procedures.  You have to pay thousands of dollars to see the latest SQL standard.  E. F. Codd's original paper is 11 pages long and is available for free.[2]  Unsurprisingly that makes it much easier to reason about.  I figure gophers should find that sentiment familiar - good theory has had "features" added to it until it mutates into something else. [3]

I am sure many people will find the package hideous.  Personally I vacillate between excitement over how interesting I find it, and despair over how much I've fallen short of the simplicity of relational algebra.  This implementation violates practically every piece of general advice I've seen on Go, but at the same time, I have a sense that it is coming together.  I hope you find it interesting as well, and I would like to hear your ideas about how to do it better.

[1] Parallel Database Systems: The Future of Database Processing or a Passing Fad?
[2] http://www.seas.upenn.edu/~zives/03f/cis550/codd.pdf
[3] http://swtch.com/~rsc/regexp/regexp1.html
