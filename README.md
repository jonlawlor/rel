rel
========

Relational Algebra in Go.

This implements most (?) of the elements of relational algebra, including project, restrict, join, intersect, setdiff, and union.  It also implements some of the common non-relational operations, including groupby, order, insert, and update.  To learn more about relational algebra, C. J. Date's Database in Depth is a great place to start, and is used as the source of terminology in the rel package.

The semantics of this package are very similar to Microsoft's LINQ, although the syntax is different.

Interfaces
==========
The uses of the interfaces defined in the rel package are outlined here.

Relation Interface
------------------

Relations are channels of tuples, and operations on those channels.  The relational algebra operations of project, restrict, join, intersect, setdiff, and union all take at least one relation input and result in a relation output.

Predicate Interface
-------------------
Predicates are used in the restrict operation.

TODOs
=====
+ Query rewrite using the rules of relational algebra, for example: http://www.dcs.warwick.ac.uk/~wmb/CS319/pdf/opt.pdf gives a quick summary.
+ Use the go race detector & clear up any issues.  (this requires a 64bit arch)
+ Implement tests with deterministic output of relational operations.  Currently tests for things like GoString, join, and groupby are dependent on the (arbitrary) order of output tuples.  They should go through an orderby operation first, or just compare against a known good relation through setdiff.
+ Add benchmarks in each of the tests.
+ Make String() function to provide shorter representations.
+ Implement sub packages for other data sources, such as csv readers, generic sql tables, json, or gob.
+ Implement non-adhoc predicates.
+ Implement non relational operations like update, insert, & order.
+ Write better docs
+ Write single godoc file
+ Implement channel cancelling
+ Use the errors package for error handling
