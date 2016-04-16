/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  //OK,
  //Insert Record Failed (SortedPage::insertRecord),
  //Delete Record Failed (SortedPage::deleteRecord,
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord (AttrType key_type,
                                 char * recPtr,
                                 int recLen,
                                 RID& rid)
{
    int i; // using i to iterate the slots
    int j;
    int offset;
    
    if (available_space() < recLen)
        return DONE;
    
    for (i = 0; i < slotCnt; ++i) {
        if (keyCompare(recPtr, &data[slot[i].offset], key_type) > 0) {
            continue;
        } else {
            offset = slot[i].offset;
            // update the offset for slots after slot_n
            for (j = i; j < slotCnt; ++j) {
                slot[j].offset -= recLen;
            }
            memmove(&slot[i+1], &slot[i], sizeof(slot_t)*(slotCnt-i));
            memmove(data+usedPtr-recLen, data+usedPtr, slot[i].offset+slot[i].length-usedPtr);
            break;
        }
    }
    
    rid.pageNo = curPage;
    rid.slotNo = i;
    memcpy(&data[offset], recPtr, recLen);
    slot[i].offset = offset;
    slot[i].length = recLen;
    
    ++slotCnt;
    usedPtr -= recLen;
    freeSpace -= recLen;
    
    return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
    int i;
    int slot_n = rid.slotNo;
    if (slot_n >= slotCnt)
        return FAIL;
    
    slot_t *curr = slot + slot_n;
    int recLen = curr->length;
    int offset = curr->offset;
    memmove(data+usedPtr+recLen, data+usedPtr, offset-usedPtr);
    
    // update the offset for slots after slot_n
    for (i = slot_n+1; i < slotCnt; ++i) {
        slot[i].offset += recLen;
    }
    
    memmove(&slot[slot_n], &slot[slot_n+1], sizeof(slot_t))
    
    --slotCnt;
    usedPtr += recLen;
    freeSpace += (recLen + sizeof(slot_t));
    return OK;
}

int SortedPage::numberOfRecords()
{
  return slotCnt;
}
