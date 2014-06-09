rel
========

Relational Algebra in Go.  Most Go doesn't need relational algebra, but most relational algebra needs Go.  The inherent parallelism in relational operations is buried by RDBMS's, and is much less extensible in function.  The cleanness of expression is missing in most ORMs, which prevents most query optimization.  Go provides the tools which can take advantage of both that parallelism and query optimization.  It is my hope that this package will prove that is true.

This implements most of the traditional elements of relational algebra, including project, restrict, join, intersect, setdiff, and union.  It also implements some of the common non-relational operations, including groupby, map, and order.  To learn more about relational algebra, C. J. Date's Database in Depth is a great place to start, and is used as the source of terminology in the rel package.

The semantics of this package are very similar to Microsoft's LINQ, although the syntax is somewhat different.  This isn't LINQ though - it is a library, and it is not integrated with the language, which means rel has a significant performance cost relative to normal go code that doesn't use reflection.  On the plus side, you can see how it does everything.

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
+ Implement tests for Err()
+ Implement tests for channel canceling
+ Implement tests for Map
+ Use the go race detector & clear up any issues.  (this requires a 64bit arch)
+ Reach 100% test coverage (currently 60%)
+ Implement tests with deterministic output of relational operations.  Currently tests for things like GoString, join, and groupby are dependent on the (arbitrary) order of output tuples.  They should go through an orderby operation first, or just compare against a known good relation through setdiff.
+ Implement sub packages for other data sources, such as csv readers, generic sql tables, json, or gob.
+ Implement non relational operations like order.
+ Write better docs
+ Write single godoc file
+ Hook up chan_mem to some kind of copying mechanism.

Errors
======
I'm not sure what to do with errors yet.  There are 2 types of errors that can be handled: errors in relational construction, like projecting a relation to a set of tuples that are not a subset of the original relation, and errors during computation, like when a data source unexpectedly disconnects.  There are two types of error handling available to us: either panic (and maybe recover) which is expensive, or having some kind of Error() method of relations, which returns an error.  If no error has been encountered, then Error should return nil, otherwise some kind of formatted string.  Having 2-arg outputs is not conducive to the method chaining that is used.

I think the best course of action is to reserve panic for problems that are not possible for the rel package to handle in advance - like a type error in an AdHoc predicate or grouping function, which rel has no control over.

To that end, rel will go the Error() route, which will be checked in the following places:

1) during derived relational construction, if one of the source relations is an error, then that relation will be returned instead of the compound relation.  In the case that two relations are provided and both are errors, then the first will be returned.
2) in the tuples method, if the source(s) of tuples are closed, then they are checked for an error.  If it is not nil, then the error is set in the derived relation, and the results channel is closed.
3) in the tuples method, if the relation already has non-nil error, then the results channel is immediately closed.