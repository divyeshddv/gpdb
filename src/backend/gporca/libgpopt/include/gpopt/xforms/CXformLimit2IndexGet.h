//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLimit2IndexGet.h
//
//	@doc:
//		Transform select over table into an index get
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLimit2IndexGet_H
#define GPOPT_CXformLimit2IndexGet_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformLimit2IndexGet
	//
	//	@doc:
	//		Transform select over a table into an index get
	//
	//---------------------------------------------------------------------------
	class CXformLimit2IndexGet : public CXformExploration
	{

		private:

			// private copy ctor
			CXformLimit2IndexGet(const CXformLimit2IndexGet &);

			// return the column reference set of included / key columns
			CColRefSet *GetColRefSet
				(
				CMemoryPool *mp,
				CLogicalGet *popGet,
				const IMDIndex *pmdindex,
				BOOL fIncludedColumns
				)
				const;

		public:

			// ctor
			explicit
			CXformLimit2IndexGet(CMemoryPool *mp);

			// dtor
			virtual
			~CXformLimit2IndexGet()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLimit2IndexGet;
			}

			// xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformLimit2IndexGet";
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// actual transform
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;


	}; // class CXformLimit2IndexGet

}

#endif // !GPOPT_CXformLimit2IndexGet_H

// EOF
