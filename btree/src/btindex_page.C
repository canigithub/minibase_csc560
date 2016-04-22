/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"
#include <assert.h>

#ifndef CHECK_STATUS
#define CHECK_STATUS if(status != OK) {return status;}
#endif

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages,
  //OK,
  //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
    KeyDataEntry *record;  
    Datatype data;
    data.pageNo = pageNo;
    int recLen;
    
    make_entry(record, key_type, key, INDEX, data, &recLen);
    status = SortedPage::insertRecord(key_type, (char *)record, recLen, rid);
    if (status != OK && status != DONE) {
        cerr << "error - btindex_page.C - insertKey\n";
    }
    return status;
}

Status BTIndexPage::insertEntry(const void *key, 
                                AttrType key_type, 
                                RID& rid, 
                                PageId &sibIndexPageId)
{
    Status status;
    sibIndexPageId = -1;
    Keytype *key_;
    Datatype *data_;
    PageId tarPageId;
    
    int i = 0, cmp;
    
    for (; i < slotCnt-1; ++i) {
        get_key_data(key_, data_, &data[slot[i+1].offset], slot[i+1].length, INDEX);
        cmp = keyCompare(key, key_, key_type);
        if (cmp < 0) break; 
    }
    
    tarPageId = data_->pageNo;
    SortedPage *targetPage;
    int sibPageId = -1;
    status = MINIBASE_BM->pinPage(tarPageId, (Page *&)targetPage, 0); CHECK_STATUS
    short target_pg_type = targetPage->get_type();
    if (target_pg_type == INDEX) {
        status = (BTIndexPage *)targetPage->insertEntry(key, key_type, rid, sibPageId);
    } else if (target_pg_type == LEAF) {
        status = (BTLeafPage *)targetPage->insertEntry(key, key_type, rid, sibPageId);
    } else {
        cerr << "error";
        exit(1);
    }
    
    status = MINIBASE_BM->unpinPage(tarPageId, 1, 0); CHECK_STATUS
    
    if (sibPageId == -1) {
      // if child has enough space  
      return OK;
    }
    
    SortedPage *sibling;
    status = MINIBASE_BM->pinPage(sibPageId, sibling, 0); CHECK_STATUS
    
    Keytype *sibFirstKey = (Keytype *)malloc(sizeof(Keytype));
    short sib_type = sibling->get_type();
    
    assert(sib_type == target_pg_type);
    
    if (sib_type == LEAF) {
      RID d_rid1, d_rid2;
      status = (BTLeafPage *)sibling->get_first(d_rid1, sibFirstKey, d_rid2); CHECK_STATUS
    } else if (sib_type == INDEX){
      RID f_rid;
      PageId d_pid;
      status = (BTIndexPage *)sibling->get_first(f_rid, sibFirstKey, d_pid); CHECK_STATUS
      status = sibling->deleteRecord(f_rid); CHECK_STATUS
    } else {
      cerr << "error at btindex.C";
      exit(1);
    }
    status = MINIBASE_BM->unpinPage(sibPageId, 1, 0); CHECK_STATUS
    
    
    RID d_rid;
    status = insertKey(sibFirstKey, key_type, sibPageId, d_rid); CHECK_STATUS
    
    if (status == OK) {
      // successfully inserted key into current index page
       return OK;
    }
    
    splitIndexPage(key_type, sibIndexPageId);
    free(sibFirstKey);
    return OK; 
}

Status BTIndexPage::splitIndexPage(AttrType key_type, PageId &sibPageId) {
    
    Status status;
    BTIndexPage *sibling;
    
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
        status = sibling->insertRecord(key_type, (char *)kde, recLen, d_rid); CHECK_STATUS;
    }
    
    for (i = slotCnt-1; i >= slotCnt/2; --i) {
        RID curRid;
        curRid.pageNo = curPage;
        curRid.slotNo = i;
        status = deleteRecord(curRid);
    }
    
    status = MINIBASE_BM->unpinPage(sibPageId, 1, 0); CHECK_STATUS
    return OK;
    
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
    Status status;
  
    void *key_;
    Datatype *data_;
    PageId tarPageId;
    
    int i = 0, cmp;
    int condemned = -1;
    
    for (; i < slotCnt-1; ++i) {
        get_key_data(key_, data_, &data[slot[i+1].offset], slot[i+1].length, INDEX);
        cmp = keyCompare(key, key_, key_type);
        if (cmp == 0) {
          condemned = i+1;
          break;
        } else if (cmp < 0) {
          break;
        }
    }
    
    if (condemned != -1) {
      RID curRid;
      curRid.pageNo = curPage;
      curRid.slotNo = condemned;
      status = deleteRecord(curRid); CHECK_STATUS
    }
    
    return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
  
//     int lo = 0, hi = slotCnt-1, mid = -1;
    // int cmp = 0;
    // void *key_;
    // Datatype *data_;
//     while (lo <= hi) {
//       mid = lo + (hi-lo)/2;
//       get_key_data(key_, data_, &data[slot[mid].offset], slot[mid].length, INDEX);
//       cmp = keyCompare(key, key_, key_type);
//       if (cmp == 0) {
//           pageNo = data_->pageNo;
//           return OK;
//       } else if (cmp < 0) {
//           hi = mid-1;
//       } else {
//           lo = mid+1;
//       }
//   }
//   // didn't find it
//   return FAIL;
    int i = 1;
    int cmp = 0;
    void *key_;
    Datatype *data_;
    
    for (; i < slotCnt; ++i) {
        get_key_data(key_, data_, &data[slot[i].offset], slot[i].length, INDEX);
        cmp = keyCompare(key, key_, key_type);
        if (cmp > 0) {
            continue;
        } else if (cmp < 0) {
            get_key_data(key_, data_, &data[slot[i-1].offset], slot[i-1].length, INDEX);
            pageNo = data_->pageNo;
            return OK;
        } else {
            pageNo = data_->pageNo;
            return OK;
        }
    }
    
    pageNo = data_->pageNo;
    return OK;
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
    Datatype *data_;
    get_key_data(key, data_, &data[slot[0].offset], slot[0].length, LEAF);
    pageNo = data_->pageNo;
    rid.pageNo = curPage;
    rid.slotNo = 0;
    return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
    assert(rid.pageNo == curPage);
    int slotNo = rid.slotNo+1;
    if (slotNo > slotCnt) {
        return FAIL;
    } else if (slotNo == slotCnt) {
        return DONE;
    } 
    Datatype *data_;
    get_key_data(key, data_, &data[slot[slotNo].offset], slot[slotNo].length, INDEX);
    pageNo = data_->pageNo;
    rid.slotNo = slotNo;
    return OK;
}

Status BTIndexPage::delete_dataRid(const void *key, const RID rid, AttrType key_type) {
    
    Status status;
    PageId targetPageId;
    status = get_page_no(key, key_type, targetPageId); CHECK_STATUS
    SortedPage *targetPage;
    status = MINIBASE_BM->pinPage(targetPageId, (Page *&)targetPage; 0); CHECK_STATUS
    short target_pg_type = targetPage->get_type();
    if (target_pg_type == INDEX) {
        status = (BTIndexPage *)targetPage->delete_dataRid(key, rid, key_type);
        status = MINIBASE_BM->unpinPage(targetPageId, 0, 1); CHECK_STATUS
    } else {
        status = (BTLeafPage *)targetPage->delete_dataRid(key, rid, key_type);
        status = MINIBASE_BM->unpinPage(targetPageId, 1, 1); CHECK_STATUS
    }
    
    return status;
}
