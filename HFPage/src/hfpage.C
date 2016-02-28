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
		int i;
		int slot_n;
		slot_t *curr;
    if(available_space() < recLen)
			return DONE;

		// find an empty slot
		// int slot_n = findEmptySlot();
		slot_n = ALL_SLOTS_FULL;
		if(slotCnt > 0) {
			if(slot[0].length == EMPTY_SLOT) {
				slot_n = 0;
			} else {
				for(i = 1; i < slotCnt; i++) {
					curr = (slot_t*) (data + i*sizeof(slot_t));
					if(curr->length == EMPTY_SLOT) {
						slot_n = i;
						break;
					}
				}
			}
		}
		
		if(slot_n == ALL_SLOTS_FULL) {				// Add a new slot and add data to `front` of data[]
			usedPtr -= recLen;
			memcpy(data+usedPtr, recPtr, recLen);
	
			slot_t *newSlot = slotCnt ? (slot_t*) (data + (slotCnt-1)*sizeof(slot_t)) : slot;
			newSlot->offset = usedPtr;
			newSlot->length = recLen;
			freeSpace -= sizeof(slot_t);
			
			rid.slotNo = slotCnt; 			// syntax for references bothers me...
			slotCnt++;
	
		} else {
			rid.slotNo = slot_n;
			slot_t* s = slot_n ? (slot_t*) (data + (slot_n-1) * sizeof(slot_t)) : slot; 

			// shiftRecordOffsets(s+1, -recLen);
			for(i = slot_n+1; i < slotCnt; i++) {
				slot_t* curr = (slot_t*) (data + i*sizeof(slot_t));
				curr->offset -= recLen;
			}

			memmove(data + usedPtr-recLen, data + usedPtr, s->offset - usedPtr);		// critical: s->offset must be equal to the offset of the `next` record
			s->length = recLen;																										// (the one with the previous slot number, next in memory)
			s->offset -= recLen;

			memcpy(data + s->offset, recPtr, recLen);
		}

		rid.pageNo = curPage;
		freeSpace -= recLen;
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    if (empty()) return DONE;
	int i;
	int slot_n = rid.slotNo;
	if (slot_n >= slotCnt) return DONE; // slot_n out of range
	slot_t* curr = slot_n ? (slot_t*) (data + (slot_n-1)*sizeof(slot_t)) : slot;
	int recLen = curr->length;
    if (recLen == EMPTY_SLOT) return DONE;
	char* recPtr = curr->offset;
    curr->length = EMPTY_SLOT;
	memmove(data + usedPtr + recLen, data + usedPtr, recPtr - usedPtr);
	usedPtr += recLen;
	freeSpace += recLen;

	if (slot_n == slotCnt - 1) { // if slot_n is the last slot
		if (slot_n > 0) {
            for (i = slotCnt-2; i >= 0; --i) { // cascade eliminate empty slots at the back 
                curr = (slot_t*) (data + i*sizeof(slot_t));
                if (curr->length == EMPTY_SLOT) {--slotCnt; freeSpace += sizeof(slot_t);}
                else break;
            }
        }
		else slot->length = EMPTY_SLOT;
	} else {
		curr->length = EMPTY_SLOT;
        for (i = slot_n; i < slotCnt-1; ++i) {
            curr = (slot_t*) (data + i*sizeof(slot_t));
            curr->offset += recLen;
        }
	}
	
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    if (empty()) return DONE;
    int i;
    slot_t* curr;
    firstRid.pageNo = curPage;
    if (slot[0]->length != EMPTY_SLOT) {firstRid.slotNo = 0; return OK;}
    else {
        for (i = 0; i < slotCnt-1; ++i) {
            curr = (slot_t*) (data + i*sizeof(slot_t));
            if (curr->length != EMPTY_SLOT) {firstRid.slotNo = i+1; return OK;}
        }
    }
    return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    if (empty()) return DONE;
    int i;
    slot_t* curr;
    int slot_n = curRid.slotNo;
    if (slot_n >= slotCnt - 1) return DONE; // no 'next' exists
    nextRid.pageNo = curPage;
    for (i = slot_n; i < slotCnt-1; ++i) {
        curr = (slot_t*) (data + i*sizeof(slot_t));
        if (curr->length != EMPTY_SLOT) {nextRid.slotNo = i+1; return OK;}
    }
    return DONE; // all slots after curRid are empty
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    if (empty()) return DONE;
    int i;
    slot_t* curr;
    int slot_n = rid.slotNo;
    if (slot_n >= slotCnt) return DONE;
    curr = slot_n ? (slot_t*) (data + (slot_n-1)*sizeof(slot_t)) : slot;
    if (curr->length == EMPTY_SLOT) return DONE;
    recPtr = curr->offset;
    recLen = curr->length;
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
    return freeSpace;   
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
//
// Since we need to use slotCnt to iterate over all the slots,
// slotCnt must not decrement when we delete a record.
bool HFPage::empty(void)
{
	if(slotCnt == 0)		
		return true;
	if(slot[0].length != EMPTY_SLOT)
		return false;
	for(int i = 0; i < slotCnt; i++) {
		if(slot[i].length != EMPTY_SLOT)
			return false;
	}
	return true;

    
		/*if (slotCnt > 1) return false;
    if (slot[0]->length != EMPTY_SLOT) return false;    // if slotCnt = 1
    return true;
		*/
}



