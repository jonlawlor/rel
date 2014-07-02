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
