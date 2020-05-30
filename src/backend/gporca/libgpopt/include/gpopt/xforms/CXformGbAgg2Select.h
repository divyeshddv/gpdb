//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformGbAgg2Select.h
//
//	@doc:
//		Split an aggregate into a pair of local and global aggregate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAgg2Select_H
#define GPOPT_CXformGbAgg2Select_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformGbAgg2Select
	//
	//	@doc:
	//		Split an aggregate operator into pair of local and global aggregate
	//
	//---------------------------------------------------------------------------
	class CXformGbAgg2Select : public CXformExploration
	{

		private:

			// private copy ctor
			CXformGbAgg2Select(const CXformGbAgg2Select &);

		protected:

			// check if the transformation is applicable;
			static
			BOOL FApplicable(CExpression *pexpr);

			// generate a project lists for the local and global aggregates
			// from the original aggregate
			static
			void PopulateLocalGlobalProjectList
					(
					CMemoryPool *mp, // memory pool
					CExpression *pexprProjListOrig, // project list of the original global aggregate
					CExpression **ppexprProjListLocal, // project list of the new local aggregate
					CExpression **ppexprProjListGlobal // project list of the new global aggregate
					);

		public:

			// ctor
			explicit
			CXformGbAgg2Select(CMemoryPool *mp);

			// ctor
			explicit
			CXformGbAgg2Select(CExpression *pexprPattern);

			// dtor
			virtual
			~CXformGbAgg2Select()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfSplitGbAgg;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformGbAgg2Select";
			}

			// Compatibility function for splitting aggregates
			virtual
			BOOL FCompatible(CXform::EXformId exfid)
			{
				return ((CXform::ExfSplitDQA != exfid) &&
						(CXform::ExfSplitGbAgg != exfid) &&
						(CXform::ExfEagerAgg != exfid));
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp (CExpressionHandle &exprhdl) const;

			// actual transform
			virtual
			void Transform
				(
				CXformContext *pxfctxt,
				CXformResult *pxfres,
				CExpression *pexpr
				)
				const;

	}; // class CXformGbAgg2Select

}

#endif // !GPOPT_CXformGbAgg2Select_H

// EOF
