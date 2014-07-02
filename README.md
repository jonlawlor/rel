rel
===

Relational Algebra in Go.  Go's interfaces & duck typing are used to provide an extensible ORM that is capable of query rewrite, and can perform relational operations both in native go and on source dbms's. Go's concurrency mechanisms (will) allow for fine control of the inherent parallelism in relational operations.  It is my hope that this package will produce some interesting approaches to implement relational expressions.  This package is currently experimental and its interfaces may change.

This implements most of the traditional elements of relational algebra, including project, restrict, join, set difference, and union.  It also implements some of the common non-relational operations, including groupby, and map.  To learn more about relational algebra, C. J. Date's Database in Depth is a great place to start, and it is used as the source of terminology in the rel package.

Please note that relational algebra *_is not SQL_*.  In particular, NULL is not a part of relational algebra, and all relations are distinct.

The semantics of this package are very similar to Microsoft's LINQ, although the syntax is somewhat different.  rel provides a uniform interface to many different types of data sources.  This isn't LINQ though - it is a library, and it is not integrated with the language, which means rel has a significant performance cost relative to normal go code that doesn't use reflection.  It also reduces the type safety.  At some point in the future, it might include code generation along the same lines as the gen package (http://clipperhouse.github.io/gen/) and the megajson package (https://github.com/benbjohnson/megajson).

Installation
============
This package can be installed with the go get command:
```
go get github.com/jonlawlor/rel
```

API Documentation
=================
http://godoc.org/github.com/jonlawlor/rel

Project Priorities
==================
1) Faithful representation of relational algebra
2) Extensibility
3) Developer friendliness
4) Performance

Thanks
======
* Andrew Janke
* Ben Johnson

TODOs
=====
+ Reach 100% test coverage (currently 85%)
+ Implement benchmarks in both "normal" rel reflection and native equivalents to determine reflection overhead
+ Implement sub packages for other data sources, such as json or gob.  A distributed relational algebra?
+ Implement non relational operations like order?
+ Hook up chan_mem to some kind of copying mechanism
+ Should attributes have an associated type, or just a name like it is now?

Errors & Cancellation
=====================
There are two ways (other than program termination) that relational queries can be terminated early: errors, which propogates "downstream" from the tuple sender, and cancellation, which propagates "upstream" from the tuple receiver.

There are 2 types of errors that can be handled: errors in relational construction, like projecting a relation to a set of tuples that are not a subset of the original relation, and errors during computation, like when a data source unexpectedly disconnects.  There are two types of error handling available to us: either panic (and maybe recover) which is expensive, or having an Err() method of relations, which returns an error.  If no error has been encountered, then Err should return nil, otherwise an error.  The rel package tries to find errors early, and avoid panic as much as possible. Having 2-arg outputs is not conducive to the method chaining currently used.  The Err() method way of handling errors is also used in the sql package's Scanner, so there is some precedent.

Therefore, you should check for errors in the following places:

1) during derived relational construction, if one of the source relations is an error, then that relation will be returned instead of the compound relation.  In the case that two relations are provided and both are errors, then the first will be returned.
2) after the chan used in TupleChan, if the source(s) of tuples are closed, then you should check for an error.
3) if you implement your own relation, you should check for an Err() after a source closes, or set it if you have encountered one (and close the results channel).

Cancellation is handled in the Tuples method.  If a caller no longer wants any results, they should close the cancel channel, which will then stop tuples from being sent by the Tuples method, which will also relay the cancellation up to any sources of tuples that it is consuming.  It will _not_ close the results channel.

Draft Golang Nuts Announcement
==============================
[ANN] Relational Algebra

rel is a relational algebra package for Go, available at https://github.com/jonlawlor/rel.  It started out as an attempt to write relational algebra in Go, and ended up as an ORM which can perform extensible query rewrite, and can perform relational operations both in native Go and on source DBMS's.  Relations are implemented as pipelines of tuples that are transformed and composed to produce results through a channel using reflection.  The package is currently experimental, interfaces are subject to change, and you should not use it for anything requiring even medium performance.  

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
	R  int
	M  int
	VA float64
}
type multElemB struct {
	M  int
	C  int
	VB float64
}
type multElemC struct {
	R  int
	C  int
	M  int
	VA float64
	VB float64
}
type groupTup struct {
	VA float64
	VB float64
}
type valTup struct {
	V float64
}
groupAdd := func(val <-chan groupTup) valTup {
	res := valTup{}
	for vi := range val {
		res.V += vi.VA * vi.VB
	}
	return res
}

// representation of a matrix:
//  1 2
//  3 4
A := New([]matrixElem{
	{1, 1, 1.0},
	{1, 2, 2.0},
	{2, 1, 3.0},
	{2, 2, 4.0},
}, [][]string{[]string{"R", "C"}})

// representation of a matrix:
//  4 17
//  9 17
B := New([]matrixElem{
	{1, 1, 4.0},
	{1, 2, 17.0},
	{2, 1, 9.0},
	{2, 2, 17.0},
}, [][]string{[]string{"R", "C"}})

C := A.Rename(multElemA{}).Join(B.Rename(multElemB{}), multElemC{}).
	GroupBy(matrixElem{}, groupAdd)
	
fmt.Println("\n%#v",C)

// Output: (might be in any order)
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

It isn't going to set any records for brevity or efficiency.  It demonstrates some of the issues with the rel package's current state: it results in a lot of type definitions for intermediate tuple representation.  Behind the scenes there is quite a bit of reflection as well, which comes with other downsides: slower performance and less type safety.  Above, every relational operation results in a new pipeline stage getting built, with more channels handing off values.  Some of the operations result in parallel execution; in this case the join operation has runtime.MaxConcurrent goroutines each performing a parallel nested loop join, and during the groupby, each group gets its own goroutine, also executed in parallel.  Neither the nested loop join, or per-result goroutine groupby can be used for large data sets.

Users of this package can define their own Relation(s), which could be used in a similar manner to LINQ's Type Providers.  The "literal" Relations above can be replaced by Relations that provide values from an SQL database (https://github.com/jonlawlor/relsql), or a csv.Reader (https://github.com/jonlawlor/relcsv), or in a user defined source if they roll their own Relation, which can take advantage of the query rewrite rules in the rel package.

Why care about relational algebra over SQL?  Relational algebra is much, much smaller and simpler than the SQL standard.  The current SQL standard is hundreds of pages long and defines everything from regular expressions to recursive queries to interaction with Java procedures.  To make things worse, every flavor of database has its own quirks, and presenting a uniform interface to them is quite difficult[2].  E. F. Codd's original paper is 11 pages long and is available for free.[3]  Unsurprisingly that makes it much easier to reason about.  I figure gophers should find that sentiment familiar - good theory has had features added to it until it is quite difficult to get to the core ideas. [4]

I am sure many people will find the package hideous.  Personally I vacillate between excitement over how interesting I find it, and distress over how ugly it gets under the hood.  This implementation violates practically every piece of general advice I've seen on Go, but at the same time, I have a sense that something useful is coming together.  I hope you find it interesting as well, and I would like to hear your ideas about how to do it better, and what direction to go from here.

[1] Parallel Database Systems: The Future of Database Processing or a Passing Fad?
[2] https://github.com/jmoiron/sqlx
[3] http://www.seas.upenn.edu/~zives/03f/cis550/codd.pdf
[4] http://swtch.com/~rsc/regexp/regexp1.html