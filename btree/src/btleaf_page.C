/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btleaf_page.h"
#include <assert.h>

#ifndef CHECK_STATUS
#define CHECK_STATUS if(status != OK) {return status;}
#endif 

const char* BTLeafErrorMsgs[] = {
// OK,
// Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{
    Status status;
    KeyDataEntry *record;
    Datatype data;
    data.rid = dataRid;
    int *recLen = (int *)malloc(sizeof(int));
    make_entry(record, key_type, key, LEAF, data, recLen);
    status = SortedPage::insertRecord(key_type, (char *)record, *recLen, rid);
    if (status != OK && status != DONE) {
        cerr << "error - btleaf_page.C - insertRec\n";
    }
    return status;
}

Status BTLeafPage::insertEntry(const void *key, 
                               AttrType key_type, 
                               RID dataRid, 
                               RID& rid, 
                               PageId &sibPageId)
{
    Status status;
    sibPageId = -1;
    status = insertRec(key, key_type, dataRid, rid);
    if (status == OK) {
        return OK;
    }
    
    // handle split
    status = splitLeafPage(key_type, sibPageId); CHECK_STATUS
    RID d_rid1, d_rid2;
    Keytype *sibFirstKey = (Keytype *)malloc(sizeof(Keytype));
    BTLeafPage *sibling;
    status = MINIBASE_BM->pinPage(sibPageId, sibling, 0); CHECK_STATUS
    status = sibling->get_first(d_rid1, sibFirstKey, d_rid2); CHECK_STATUS
    int cmp = keyCompare(key, sibFirstKey, key_type);
    if (cmp < 0) {
        // new entry go here
        status = insertRec(key, key_type, dataRid, rid); CHECK_STATUS
    } else {
        // new entry go to sibling
        status = sibling->insertRec(key, key_type, dataRid, rid); CHECK_STATUS
    }
    status = MINIBASE_BM->unpinPage(sibPageId, 1, 0); CHECK_STATUS
}

Status BTLeafPage::splitLeafPage(AttrType key_type, 
                                 PageId &sibPageId)
{   
    Status status;
    status = MINIBASE_DB->allocate_page(sibPageId, 1); CHECK_STATUS
    status = MINIBASE_BM->pinPage(sibPageId, sibling, 1); CHECK_STATUS
    sibling->init(sibPageId);
    int i = slotCnt/2;
    for (; i < slotCnt; ++i) {
        KeyDataEntry *kde;
        RID curRid;
        curRid.pageNo = curPage;
        curRid.slotNo = i;
        int recLen;
        status = HFPage::getRecord(curRid, (char *)kde, recLen); CHECK_STATUS
        RID d_rid;
        status = SortedPage::insertRecord(key_type, (char *)kde, recLen, d_rid); CHECK_STATUS;
    }
    
    for (i = slotCnt-1; i >= slotCnt/2; --i) {
        RID curRid;
        curRid.pageNo = curPage;
        curRid.slotNo = i;
        status = SortedPage::deleteRecord(curRid);
    }
    
    PageId oldNextPageId = getNextPage();
    if (oldNextPageId != -1) {
        BTLeafPage *oldNextPage;
        status = MINIBASE_BM->pinPage(oldNextPageId, oldNextPage, 0); CHECK_STATUS
        oldNextPage->setPrevPage(sibPageId);
        status = MINIBASE_BM->unpinPage(oldNextPageId, 1, 0); CHECK_STATUS
    }
    
    sibling->setNextPage(oldNextPageId);
    sibling->setPrevPage(curPage);
    setNextPage(sibPageId);
    
    status = MINIBASE_BM->unpinPage(sibPageId, 1, 0); CHECK_STATUS
    return OK;
}


/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)
{
    int lo = 0, hi = slotCnt-1, mid = -1;
    int cmp = 0;
    void *key_;
    Datatype *data_;
    while (lo <= hi) {
        mid = lo + (hi-lo)/2;
        get_key_data(key_, data_, &data[slot[mid].offset], slot[mid].length, LEAF);
        cmp = keyCompare(key, key_, key_type);
        if (cmp == 0) {
            dataRid = data_->rid;
            return OK;
        } else if (cmp < 0) {
            hi = mid-1;
        } else {
            lo = mid+1;
        }
    }
    // didn't find it
    return FAIL;
}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 
    Datatype *data_;
    get_key_data(key, data_, &data[slot[0].offset], slot[0].length, LEAF);
    dataRid = data_->rid;
    rid.pageNo = curPage;
    rid.slotNo = 0;
    return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{
    assert(rid.pageNo == curPage);
    int slotNo = rid.slotNo+1;
    if (slotNo > slotCnt) {
        return FAIL;
    } else if (slotNo == slotCnt) {
        return DONE;
    } 
    Datatype *data_;
    get_key_data(key, data_, &data[slot[slotNo].offset], slot[slotNo].length, LEAF);
    dataRid = data_->rid;
    rid.slotNo = slotNo;
    return OK;
}
