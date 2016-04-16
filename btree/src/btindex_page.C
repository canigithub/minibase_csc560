/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"
#include <assert.h>

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
    void *key_;
    Datatype *data_;
    PageId tarPage = -1;
    int i, cmp;
    Status st;
    
    for (i = 0; i < slotCnt-1; ++i) {
        get_key_data(key_, data_, &data[slot[i+1].offset], slot[i+1].length, INDEX);
        cmp = keyCompare(key, key_, key_type);
        if (cmp < 0) break;
    }
    tarPage = data_->pageNo;
    SortedPage *child; 
    st = MINIBASE_BM->pinPage(tarPage, child, 0);
    short pg_type = child->get_type();
    if (pg_type == INDEX) {
        child->insertKey(key, key_type, pageNo, rid);
    } else if (pg_type == LEAF) {
      
    } else {
      
    }
     
    
    // didn't find it
    return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
  // put your code here
  return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
  // put your code here
  return OK;
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
  // put your code here
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
  // put your code here
  return OK;
}
