#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "heapfile.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
	slotCnt = 0;
	usedPtr = MAX_SPACE-DPFIXED;		// records start at end of file
	freeSpace = MAX-SPACE-DPFIXED;
	// type?
	// prevPage?
	// nextPage?
	curPage = pageNo;
			
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{

		prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
  nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
		// copy data from recPtr to data, starting backwards from the end
		char *recordBeginning = &data[usedPtr

    // create pointer to new slot directory, at the beginning of the data block
		// use slot array? not sure how that's supposed to work.  overlaps with data?
		// increment slotCnt
		// update free space
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
		// store -1 as length of slot
		// shift memory back
		// maybe we don't have to update any pointers, as the lengths are stored in the slots?
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
	return { curPage, 0 }; 
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
		if(curRid.slotNo >= slotCnt)
			return DONE;
		assert(curPage == curRid.pageNo);
    return {curPage, curRid.slotNo+1};
		return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    // Seems like we need to iterate through the slots
		// Keep track of where we are in the data array by adding the slot lengths
		// copy record content into recPtr
		// do we need to malloc recPtr with recLen?
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    // fill in the body
		// as above, iterate through until we find the location of the record
		// in fact, that function could use this one as a subroutine?
		// just return the pointer and the length
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // add the lengths of all the records (careful not to add the -1 of deleted records)
		// add the lengths of all the slots
		// subtract from initial space in data array
    return 0;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    return slotCnt == 0;
}



