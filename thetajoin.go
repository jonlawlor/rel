//thetajoin implements a Theta Join in relational algebra

package rel

type ThetaJoinExpr struct {
	source1 Relation
	source2 Relation
	p       Theta
}

// not yet implemented
