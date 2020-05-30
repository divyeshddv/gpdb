//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformGbAgg2Select.cpp
//
//	@doc:
//		Implementation of the splitting of an aggregate into a pair of
//		local and global aggregate
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefComputed.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternMultiTree.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformGbAgg2Select.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Select::CXformGbAgg2Select
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2Select::CXformGbAgg2Select
	(
	CMemoryPool *mp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(mp) CExpression
					(
					mp,
					GPOS_NEW(mp) CLogicalGbAgg(mp),
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // relational child
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Select::CXformGbAgg2Select
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2Select::CXformGbAgg2Select
	(
	CExpression *pexprPattern
	)
	:
	CXformExploration(pexprPattern)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Select::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2Select::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// do not split aggregate if it is a local aggregate, has distinct aggs, has outer references,
	// or return types of Agg functions are ambiguous
	if (!CLogicalGbAgg::PopConvert(exprhdl.Pop())->FGlobal() ||
		0 < exprhdl.DeriveTotalDistinctAggs(1) ||
		0 < exprhdl.DeriveOuterReferences()->Size() ||
		CXformUtils::FHasAmbiguousType(exprhdl.PexprScalarChild(1 /*child_index*/), COptCtxt::PoctxtFromTLS()->Pmda())
		)
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Select::Transform
//
//	@doc:
//		Actual transformation to expand a global aggregate into a pair of
//		local and global aggregate
//
//---------------------------------------------------------------------------
void
CXformGbAgg2Select::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	// check if the transformation is applicable
	if (!FApplicable(pexprProjectList))
	{
		return;
	}

	pexprRelational->AddRef();

	CExpression *pexprProjectListLocal = NULL;
	CExpression *pexprProjectListGlobal = NULL;

	(void) PopulateLocalGlobalProjectList
			(
			mp,
			pexprProjectList,
			&pexprProjectListLocal,
			&pexprProjectListGlobal
			);

	GPOS_ASSERT(NULL != pexprProjectListLocal && NULL != pexprProjectListLocal);

	
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Select::FApplicable
//
//	@doc:
//		Check if we the transformation is applicable (we currently only
//      support min max)
//
//---------------------------------------------------------------------------
BOOL
CXformGbAgg2Select::FApplicable
	(
	CExpression *pexpr
	)
{
	const ULONG arity = pexpr->Arity();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrEl = (*pexpr)[ul];

		// get the scalar child of the project element
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
        const IMDType *agg_type = md_accessor->RetrieveType(popScAggFunc->MdidType());

		if (!popScAggFunc->IsMinMax(agg_type))
		{
			return false;
		}
	}

	return true;
}

// EOF
