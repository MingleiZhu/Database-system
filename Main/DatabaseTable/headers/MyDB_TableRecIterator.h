


#ifndef TABLE_REC_ITER_H
#define TABLE_REC_ITER_H

#include "MyDB_RecordIterator.h"
#include "MyDB_Record.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_Table.h"

class MyDB_TableRecIterator : public MyDB_RecordIterator {

public:

	// put the contents of the next record in the file/page into the iterator record
	// this should be called BEFORE the iterator record is first examined
	void getNext () override;

        // BEFORE a call to getNext (), a call to getCurrentPointer () will get the address
        // of the record.  At a later time, it is then possible to reconstitute the record
        // by calling MyDB_Record.fromBinary (obtainedPointer)... ASSUMING that the page
        // that the record is located on has not been swapped out
        void *getCurrentPointer () override;

	// return true iff there is another record in the file/page
	bool hasNext () override;

	// destructor and contructor
	MyDB_TableRecIterator (MyDB_TableReaderWriter &myParent, MyDB_TablePtr myTableIn,
        	MyDB_RecordPtr myRecIn);
	~MyDB_TableRecIterator ();

private:

	MyDB_RecordIteratorPtr myIter;
	int curPage;
	
	MyDB_TableReaderWriter &myParent;
	MyDB_TablePtr myTable;
        MyDB_RecordPtr myRec;

};

#endif
