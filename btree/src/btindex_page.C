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
		Status status;
    KeyDataEntry *record = (KeyDataEntry*)malloc(sizeof(KeyDataEntry));
    Datatype data;
    data.pageNo = pageNo;
    int recLen;
    
    make_entry(record, key_type, key, INDEX, data, &recLen);			// possible suspect
    status = SortedPage::insertRecord(key_type, (char *)record, recLen, rid);
		free(record);
    if (status != OK && status != DONE) {
        cerr << "error - btindex_page.C - insertKey\n";
    }
    return status;
}

Status BTIndexPage::insertEntry(const void *key, 
                                AttrType key_type, 
																RID dataRid, 
                                RID& rid, 
                                PageId &sibIndexPageId)
{
    Status status;
    sibIndexPageId = -1;
    Keytype *key_ = (Keytype*) malloc(sizeof(Keytype));
    Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
    PageId tarPageId;
    
    int i = 0, cmp;


    for (i = 0; i < slotCnt-1; ++i) {
        get_key_data(key_, data_, (KeyDataEntry*) &data[slot[i+1].offset], slot[i+1].length, INDEX);
        cmp = keyCompare(key, key_, key_type);
        if (cmp < 0) 
						break; 
    }
    get_key_data(key_, data_, (KeyDataEntry*) &data[slot[i].offset], slot[i].length, INDEX);
    tarPageId = data_->pageNo;
    SortedPage *targetPage;


    int nephewPageId = -1;
    status = MINIBASE_BM->pinPage(tarPageId, (Page *&)targetPage, 0); CHECK_STATUS
    short target_pg_type = targetPage->get_type();
		// printf("Target page %d has type %d\n", tarPageId, target_pg_type);
		status = MINIBASE_BM->unpinPage(tarPageId, 0, 0); CHECK_STATUS
    if (target_pg_type == INDEX) {
				BTIndexPage *tarIndexPage;
				status = MINIBASE_BM->pinPage(tarPageId, (Page*&) tarIndexPage, 0);CHECK_STATUS 
        status = tarIndexPage->insertEntry(key, key_type, dataRid, rid, nephewPageId);
				status = MINIBASE_BM->unpinPage(tarPageId, 1, 0); CHECK_STATUS
    } else if (target_pg_type == LEAF) {
				BTLeafPage *tarLeafPage;
				status = MINIBASE_BM->pinPage(tarPageId, (Page*&) tarLeafPage, 0); CHECK_STATUS
        status = tarLeafPage->insertEntry(key, key_type, dataRid, rid, nephewPageId);
				status = MINIBASE_BM->unpinPage(tarPageId, 1, 0); CHECK_STATUS
    } else {
        cerr << "error" << endl;
        exit(1);
    }
    if (nephewPageId == -1) {
      // if child has enough space  
			free(key_);
			free(data_);
      return OK;
    }
		

		// there was a split at a **lower level**, have to handle it
    
    SortedPage *sibling;
    status = MINIBASE_BM->pinPage(nephewPageId, (Page*&) sibling, 0); CHECK_STATUS
    Keytype *sibFirstKey = (Keytype *)malloc(sizeof(Keytype));
    short sib_type = sibling->get_type();
    status = MINIBASE_BM->unpinPage(nephewPageId, 0, 0); CHECK_STATUS

    assert(sib_type == target_pg_type);
    
		Keytype *nephewFirstKey = (Keytype*) malloc(sizeof(Keytype));
    if (sib_type == LEAF) { // this time: 67 was created
			BTLeafPage *nephewLeafPage;
      RID d_rid1, d_rid2;
			status = MINIBASE_BM->pinPage(nephewPageId, (Page*&) nephewLeafPage, 0);
      status = nephewLeafPage->get_first(d_rid1, nephewFirstKey, d_rid2); CHECK_STATUS
			status = MINIBASE_BM->unpinPage(nephewPageId, 0, 0);
    } else if (sib_type == INDEX){
			BTIndexPage *nephewIndexPage;
      RID f_rid;
      PageId d_pid;
			status = MINIBASE_BM->pinPage(nephewPageId, (Page*&) nephewIndexPage, 0);
      status = nephewIndexPage->get_first(f_rid, nephewFirstKey, d_pid); CHECK_STATUS
			status = MINIBASE_BM->unpinPage(nephewPageId, 0, 0);
    } else {
      cerr << "error at btindex.C";
      exit(1);
    }
    
    RID d_rid;
    status = insertKey(nephewFirstKey, key_type, nephewPageId, d_rid); // tries to add sibPageId = 67, can't, returns DONE


		if (status == DONE)  {
	    	status = splitIndexPage(key_type, sibIndexPageId); // this is where we get 68 = sibIndexPage

				RID d_rid1;
				PageId d_pageNo;
				Keytype *sibFirstKey = (Keytype*) malloc(sizeof(Keytype));
				BTIndexPage *sibling;
				status = MINIBASE_BM->pinPage(sibIndexPageId, (Page*&) sibling, 0) ; CHECK_STATUS
				status = sibling->get_first(d_rid1, sibFirstKey, d_pageNo); CHECK_STATUS
				int cmp = keyCompare(nephewFirstKey, sibFirstKey, key_type);
				if(cmp < 0) {
					status = insertKey(nephewFirstKey, key_type, nephewPageId, d_rid1); CHECK_STATUS
				} else {
					status = sibling->insertKey(nephewFirstKey, key_type, nephewPageId, d_rid1); CHECK_STATUS;
				}
				status = MINIBASE_BM->unpinPage(sibIndexPageId, 1, 0); CHECK_STATUS

		}     

		free(key_);
		free(data_);
    free(sibFirstKey);
    return status;
}

Status BTIndexPage::splitIndexPage(AttrType key_type, PageId &sibPageId) {
    
    Status status;
    BTIndexPage *sibling;
    
    status = MINIBASE_DB->allocate_page(sibPageId, 1); CHECK_STATUS
    status = MINIBASE_BM->pinPage(sibPageId, (Page*&) sibling, 1); CHECK_STATUS
    sibling->init(sibPageId);
    int i = slotCnt/2;
    for (; i < slotCnt; ++i) {
        KeyDataEntry *kde = (KeyDataEntry*) malloc(sizeof(KeyDataEntry));
        RID curRid;
        curRid.pageNo = curPage;
        curRid.slotNo = i;
        int recLen;
        status = HFPage::getRecord(curRid, (char *)kde, recLen); CHECK_STATUS
        RID d_rid;
        status = sibling->insertRecord(key_type, (char *)kde, recLen, d_rid); CHECK_STATUS;
				free(kde);
    }
    
		int halfway = slotCnt/2;
    for (i = slotCnt-1; i >= halfway; --i) {
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
  
    Keytype *key_ = (Keytype*) malloc(sizeof(Keytype));
    Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
    //PageId tarPageId;
    
    int i = 0, cmp;
    int condemned = -1;
    
    for (; i < slotCnt-1; ++i) {
        get_key_data(key_, data_, (KeyDataEntry*) &data[slot[i+1].offset], slot[i+1].length, INDEX);
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
  
    int i = 1;
    int cmp = 0;
    Keytype *key_ = (Keytype*) malloc(sizeof(Keytype));
    Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
    
    for (; i < slotCnt; ++i) {
        get_key_data(key_, data_, (KeyDataEntry*) &data[slot[i].offset], slot[i].length, INDEX);
        cmp = keyCompare(key, key_, key_type);
        if (cmp > 0) {
            continue;
        } else if (cmp < 0) {
            get_key_data(key_, data_, (KeyDataEntry*) &data[slot[i-1].offset], slot[i-1].length, INDEX);
        		break;
        } else {
            break;
        }
    }
    
    pageNo = data_->pageNo;
		free(key_);
		free(data_);
    return OK;
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
    Datatype *data_ = (Datatype*) malloc(sizeof(Datatype*));
    get_key_data(key, data_, (KeyDataEntry*) &data[slot[0].offset], slot[0].length, INDEX);
    pageNo = data_->pageNo;
    rid.pageNo = curPage;
    rid.slotNo = 0;
		free(data_);
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
    Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
    get_key_data(key, data_, (KeyDataEntry*) &data[slot[slotNo].offset], slot[slotNo].length, INDEX);
    pageNo = data_->pageNo;
    rid.slotNo = slotNo;
		free(data_);
    return OK;
}

/*Status BTIndexPage::delete_dataRid(const void *key, const RID rid, AttrType key_type) {
    
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
}*/
