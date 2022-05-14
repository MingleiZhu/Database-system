

#ifndef PAGE_COMP_H
#define PAGE_COMP_H

#include "TableCompare.h"

// so that pages can be put into a map
class PageCompare {

public:

	bool operator() (const pair <MyDB_TablePtr, size_t>& lhs, const pair <MyDB_TablePtr, size_t>& rhs) const {

		TableCompare temp;
		if (temp (lhs.first, rhs.first))
			return true;

		if (temp (rhs.first, lhs.first))
			return false;

		// in this case, the tables are the same
		return lhs.second < rhs.second;
	}
};

#endif

