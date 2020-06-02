//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLimit2IndexGet.cpp
//
//	@doc:
//		Implementation of select over a table to an index get transformation
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/xforms/CXformLimit2IndexGet.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDIndexGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformLimit2IndexGet::CXformLimit2IndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformLimit2IndexGet::CXformLimit2IndexGet
	(
	CMemoryPool *mp
	)
	:
	// pattern
	CXformExploration
		(
		GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CLogicalLimit(mp),
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // relational child
                GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // scalar child for offset
                GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // scalar child for number of rows
				)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformLimit2IndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLimit2IndexGet::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLimit2IndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformLimit2IndexGet::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
    CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pexpr->Pop());
    CExpression *pexprRelational = (*pexpr)[0];
    CExpression *pexprScalarStart = (*pexpr)[1];
    CExpression *pexprScalarRows = (*pexpr)[2];
    COrderSpec *pos = popLimit->Pos();
    
    //if there is no order spec, return
    if (NULL == pos)
    {
        return;
    }
    
	// get the indexes on this relation
	CLogicalGet *popGet = CLogicalGet::PopConvert(pexprRelational->Pop());
	const ULONG ulIndices = popGet->Ptabdesc()->IndexCount();
	if (0 == ulIndices)
	{
		return;
	}
    
    // derive the scalar and relational properties to build set of required columns
    CColRefSet *pcrsOutput = pexpr->DeriveOutputColumns();
    CColRefSet *pcrsScalarExpr = pos->PcrsUsed(mp);

    CColRefSet *pcrsReqd = GPOS_NEW(mp) CColRefSet(mp);
    pcrsReqd->Include(pcrsOutput);
    pcrsReqd->Include(pcrsScalarExpr);
    
    // array of expressions in the scalar expression
    CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
    for (/* number of elements in pcrsScalarExpr*/)
    {
        pdrgpexpr->Append(GPOS_NEW(mp) CExpression(mp) /*create the expression using
                                                        the col ref*/)
    }
    GPOS_ASSERT(pdrgpexpr->Size() > 0);

	// find the indexes whose included columns meet the required columns
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(popGet->Ptabdesc()->MDId());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
		const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);
		CExpression *pexprIndexGet = CXformUtils::PexprLogicalIndexGet
						(
						 mp,
						 md_accessor,
						 pexprRelational,
						 pexpr->Pop()->UlOpId(),
						 pdrgpexpr,
						 pcrsReqd,
						 pcrsScalarExpr,
						 NULL /*outer_refs*/,
						 pmdindex,
						 pmdrel,
						 false /*fAllowPartialIndex*/,
						 NULL /*ppartcnstrIndex*/
						);
		if (NULL != pexprIndexGet)
		{
			pxfres->Add(pexprIndexGet);
		}
	}

	pcrsReqd->Release();
	pdrgpexpr->Release();
}

// EOF

