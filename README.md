rel
========

Relational Algebra in Go.

This implements most (?) of the elements of relational algebra, including project, restrict, join, theta-join, intersect, setdiff, and union.  It also implements some of the common non-relational operations, including groupby, order, insert, and update.  To learn more about relational algebra, C. J. Date's Database in Depth is a great place to start, and is used as the source of terminology in the rel package.

Interfaces
==========
The uses and semantics of the interfaces defined in the rel package are outlined here.

Relation Interface
------------------

Query Interface
---------------

Predicate Interface
-------------------
