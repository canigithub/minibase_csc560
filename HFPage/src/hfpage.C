#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


const int ALL_SLOTS_FULL = -1;

// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
		slotCnt = 0;
		usedPtr = MAX_SPACE - DPFIXED;	// work through the space in data backwards
		freeSpace = MAX_SPACE - DPFIXED;

		curPage = pageNo;
		slot[0] = NULL;
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
// Scan for an ``empty" slot that we can insert into
// The empty slot is assumed to have the same offset as the
// next filled slot.  Returns ALL_SLOTS_FULL if no empty slot 
// was found.

int HFPage::findEmptySlot() {
	int i;
	if(slot[0] == NULL)
		return ALL_SLOTS_FULL;;
	else if(slot[0].length == EMPTY_SLOT)
		return 0;
	
	for(i = 1; i < slotCnt; i++) {
		slot_t* s = (slot_t *) data[i*sizeof(slot_t)];
		if(s->length == EMPTY_SLOT) {
			return i;
		}
	}

	return ALL_SLOTS_FULL;

}

// **********************************************************
// When deleting a record R or inserting into empty slot R,
// shifts offsets of all records from R+1 to the last record
// buy the length of record R.
// IMPORTANT: Deleting moves records forwards.  A positive
// length means a positive shift, as in deleting a record.
void HFPage::shiftRecordOffsets(int firstRecord, int length) {
	int i;
	for(i = firstRecord; i < slotCnt; i++) {
		slot_t* s = (slot_t*) data[i*sizeof(slot_t)];
		s->offset += length;
	}
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
    if(available_space() < recLen)
			return DONE;

		// find an empty slot
		int slot_n = findEmptySlot();
		
		if(slot_n == ALL_SLOTS_FULL) {				// Add a new slot and add data to `front` of data[]
			usedPtr -= recLen;
			memcpy(data+usedPtr, recPtr, recLen);
	
			slot_t *newSlot = slotCnt ? ((slot_t*) data[(slotCnt-1)*sizeof(slot_t)]) : slot;
			newSlot->offset = usedPtr;
			newSlot->length = recLen;
			freeSpace -= sizeof(slot_t);
			
			rid->slotNo = slotCnt;
			slotCnt++;
	
		} else {
			rid->slotNo = slot_n;
			slot_t* s = slot_n ? ((slot_t*) data[(slot_n-1) * sizeof(slot_t)]) : slot; 

			shiftRecordOffsets(s+1, -recLen);
			memmove(data[usedPtr-recLen], data[usedPtr], s->offset - usedPtr);		// critical: s->offset must be equal to the offset of the `next` record
			s->length = recLen;																										// (the one with the previous slot number, next in memory)
			s->offset -= recLen;

			memcpy(data[s->offset], recPtr, recLen);
		}

		rid->pageNo = curPage;
		freeSpace -= recLen;
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    // fill in the body
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    // fill in the body
    return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    // fill in the body

    return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    // fill in the body
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
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
    return 0;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
    return true;
}



