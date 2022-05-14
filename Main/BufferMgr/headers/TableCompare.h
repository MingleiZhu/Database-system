

#ifndef TABLE_COMP_H
#define TABLE_COMP_H

#include "MyDB_Table.h"

// so that pages can be put into a map
struct TableCompare {

public:

	bool operator() (const MyDB_TablePtr lhs, const MyDB_TablePtr rhs) const {

		// deal with the null case
		if (lhs == nullptr && rhs != nullptr) {
			return true;
		} else if (rhs == nullptr) {
			return false;
		}

		// otherwise, just compare the strings
		return lhs->getName () < rhs->getName ();
	}
};

#endif

