



#ifndef PAGE_LIST_ITER_ALT_H
#define PAGE_LIST_ITER_ALT_H

#include "MyDB_RecordIteratorAlt.h"
#include "MyDB_PageRecIteratorAlt.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_Record.h"
#include <vector>

using namespace std;

class MyDB_PageListIteratorAlt : public MyDB_RecordIteratorAlt {

public:

        // load the current record into the parameter
        void getCurrent (MyDB_RecordPtr intoMe) override;

        // after a call to advance (), a call to getCurrentPointer () will get the address
        // of the record.  At a later time, it is then possible to reconstitute the record
        // by calling MyDB_Record.fromBinary (obtainedPointer)... ASSUMING that the page that
        // the record is located on has not been swapped out
        void *getCurrentPointer () override;

        // advance to the next record... returns true if there is a next record, and
        // false if there are no more records to iterate over.  Not that this cannot
        // be called until after getCurrent () has been called
        bool advance () override;

	// destructor and contructor
	MyDB_PageListIteratorAlt (vector <MyDB_PageReaderWriter> &forUs);
	~MyDB_PageListIteratorAlt ();

private:

	MyDB_RecordIteratorAltPtr myIter;
	vector <MyDB_PageReaderWriter> forUs;
	int curPage;
};

#endif
