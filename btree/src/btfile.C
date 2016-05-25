/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"
#include <assert.h>

#ifndef CHECK_RETURN_STATUS
#define CHECK_RETURN_STATUS if (status != OK) {returnStatus = status; return;}
#endif

#ifndef CHECK_STATUS
#define CHECK_STATUS if(status != OK) {return status;}
#endif


// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  // _OK
  // CANT_FIND_HEADER
  // CANT_PIN_HEADER,
  // CANT_ALLOC_HEADER
  // CANT_ADD_FILE_ENTRY
  // CANT_UNPIN_HEADER
  // CANT_PIN_PAGE
  // CANT_UNPIN_PAGE
  // INVALID_SCAN
  // SORTED_PAGE_DELETE_CURRENT_FAILED
  // CANT_DELETE_FILE_ENTRY
  // CANT_FREE_PAGE,
  // CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  // COULD_NOT_CREATE_ROOT
  // DELETE_DATAENTRY_FAILED
  // DATA_ENTRY_NOT_FOUND
  // CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
    Status status;
    status = MINIBASE_DB->get_file_entry(filename, headerPageId); CHECK_RETURN_STATUS
    file_name = (char*) malloc(MAX_NAME * sizeof(char));
    strcpy(file_name, filename);
    status = loadFromHeaderPage(); CHECK_RETURN_STATUS
		returnStatus = OK;
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
    Status status;
    file_name = (char*) malloc(MAX_NAME * sizeof(char));
    strcpy(file_name, filename);
        
    status = MINIBASE_DB->get_file_entry(filename, headerPageId);
    if (status == OK) {
        // if file is existing
        status = loadFromHeaderPage(); CHECK_RETURN_STATUS
        return;
    }
    
    // if file_entry not exits
    HFPage *headerPage;
    status = MINIBASE_DB->allocate_page(headerPageId, 1); CHECK_RETURN_STATUS
    status = MINIBASE_DB->add_file_entry(filename, headerPageId); CHECK_RETURN_STATUS
    status = MINIBASE_BM->pinPage(headerPageId, (Page*&)headerPage, 1); CHECK_RETURN_STATUS
    headerPage->init(headerPageId);
    status = MINIBASE_BM->unpinPage(headerPageId, 1, 0); CHECK_RETURN_STATUS
    
    // allocate rootPageId
    status = MINIBASE_DB->allocate_page(rootPageId, 1); CHECK_RETURN_STATUS
    BTLeafPage *rootPage;
    status = MINIBASE_BM->pinPage(rootPageId, (Page*&)rootPage, 1); CHECK_RETURN_STATUS
    rootPage->init(rootPageId);
    status = MINIBASE_BM->unpinPage(rootPageId, 1, 0); CHECK_RETURN_STATUS
    
    // assign key_type and key_size
    key_type = keytype;
    key_size = keysize;
    
    // save to headerPage
    status = saveToHeaderPage(); CHECK_RETURN_STATUS
		returnStatus = OK;
}

BTreeFile::~BTreeFile ()
{
  free(file_name);
}

Status BTreeFile::loadFromHeaderPage() {
    
    HFPage *headerPage;
    Status status = MINIBASE_BM->pinPage(headerPageId, (Page*&)headerPage, 0); CHECK_STATUS
    
    RID curRid;
    int recLen;
    curRid.pageNo = headerPageId;
    
    // retrieve rootPageId
    curRid.slotNo = ROOTPAGEID;
    status = headerPage->getRecord(curRid, (char *)&rootPageId, recLen); CHECK_STATUS
    
    // retrieve key_type
    curRid.slotNo = KEY_TYPE;
    status = headerPage->getRecord(curRid, (char *)&key_type, recLen); CHECK_STATUS
    
    // retrieve key_size
    curRid.slotNo = KEY_SIZE;
    status = headerPage->getRecord(curRid, (char *)&key_size, recLen); CHECK_STATUS
    
    status = MINIBASE_BM->unpinPage(headerPageId, 0, 1); CHECK_STATUS
    
    return OK;
}

Status BTreeFile::saveToHeaderPage() {
    HFPage *headerPage;
    Status status = MINIBASE_BM->pinPage(headerPageId, (Page*&)headerPage, 0); CHECK_STATUS
    
    RID d_rid;
    
    // insert rootPageId
    status = headerPage->insertRecord((char *)&rootPageId, sizeof(PageId), d_rid); CHECK_STATUS
    assert(d_rid.slotNo == ROOTPAGEID);
    
    // insert key_type
    status = headerPage->insertRecord((char *)&key_type, sizeof(AttrType), d_rid); CHECK_STATUS
    assert(d_rid.slotNo == KEY_TYPE);
    
    // insert key_size
    status = headerPage->insertRecord((char *)&key_size, sizeof(int), d_rid); CHECK_STATUS
    assert(d_rid.slotNo == KEY_SIZE);
    
    status = MINIBASE_BM->unpinPage(headerPageId, 1, 1); CHECK_STATUS
    
    return OK;
    
}

Status BTreeFile::destroyFile ()
{
  // put your code here
  return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
    SortedPage *rootPage;
    Status status = MINIBASE_BM->pinPage(rootPageId, (Page*&)rootPage, 0); CHECK_STATUS
    short pg_type = rootPage->get_type();
		status = MINIBASE_BM->unpinPage(rootPageId, 0, 0); CHECK_STATUS;
    
    RID d_rid;
    PageId sibPageId;
    
    if (pg_type == LEAF) {
				BTLeafPage *leafRoot;
				status = MINIBASE_BM->pinPage(rootPageId, (Page*&) leafRoot, 0); CHECK_STATUS
        status = leafRoot->insertEntry(key, key_type, rid, d_rid, sibPageId); CHECK_STATUS
				status = MINIBASE_BM->unpinPage(rootPageId, 1, 1);CHECK_STATUS
        
    } else if (pg_type == INDEX){
				BTIndexPage *indexRoot;
				status = MINIBASE_BM->pinPage(rootPageId, (Page*&) indexRoot, 0); CHECK_STATUS
        status = indexRoot->insertEntry(key, key_type, rid, d_rid, sibPageId); CHECK_STATUS
				status = MINIBASE_BM->unpinPage(rootPageId, 1, 1); CHECK_STATUS
    } else {
      cerr << "wrong type at btfile.C" << endl;
      exit(1);
    }
    
    if (sibPageId == -1) {
        return OK;
    }
    
		// There is a sibling page! Time to split the root page!
    PageId newRootPageId;
    status = MINIBASE_DB->allocate_page(newRootPageId, 1); CHECK_STATUS
    BTIndexPage *newRoot;
    status = MINIBASE_BM->pinPage(newRootPageId, (Page *&)newRoot, 1); CHECK_STATUS
    newRoot->init(newRootPageId);
    

    Keytype *firstKey = (Keytype *)malloc(sizeof(Keytype));
    Keytype *sibFirstKey = (Keytype *)malloc(sizeof(Keytype));
		sibFirstKey->intkey = -99;
    
    firstKey->intkey = -1;
    firstKey->charkey[4] = 0;

		if(pg_type == INDEX) {     // the root page has split, and is an index!
			RID f_rid;
			PageId d_pid;
			BTIndexPage *sibIndexPage;
			status = MINIBASE_BM->pinPage(sibPageId, (Page*&) sibIndexPage, 0);
			status = sibIndexPage->get_first(f_rid, sibFirstKey, d_pid); CHECK_STATUS
			status = MINIBASE_BM->unpinPage(sibPageId, 0, 0); CHECK_STATUS
		} else {
			RID f_rid;
			RID d_rid;
			BTLeafPage *sibLeafPage;
			status = MINIBASE_BM->pinPage(sibPageId, (Page*&) sibLeafPage, 0); CHECK_STATUS
			status = sibLeafPage->get_first(f_rid, sibFirstKey, d_rid); CHECK_STATUS 
			status = MINIBASE_BM->unpinPage(sibPageId, 0, 0); CHECK_STATUS
		}

    status = newRoot->insertKey(firstKey, key_type, rootPageId, d_rid); CHECK_STATUS
    status = newRoot->insertKey(sibFirstKey, key_type, sibPageId, d_rid); CHECK_STATUS
    
    status = MINIBASE_BM->unpinPage(newRootPageId, 1, 0); CHECK_STATUS
    rootPageId = newRootPageId;
		clearHeaderPage();
    saveToHeaderPage();
		free(firstKey);
		free(sibFirstKey);
    
    // remember to update header
    return OK;
}

Status BTreeFile::clearHeaderPage() {
		Status status;
    HFPage *headerPage;
    status = MINIBASE_BM->pinPage(headerPageId, (Page*&)headerPage, 0); CHECK_STATUS
		RID marked;
		marked.pageNo = headerPageId;
		for(int i = 2; i >= 0; i--) {
			marked.slotNo = i;	
			status = headerPage->deleteRecord(marked); CHECK_STATUS
		}
		status = MINIBASE_BM->unpinPage(headerPageId, 1, 0); CHECK_STATUS
		return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
    // call rootPage->deleteRecord    
    
    Status status;
    SortedPage *curSortedPage;
    status = MINIBASE_BM->pinPage(rootPageId, (Page *&)curSortedPage, 0); CHECK_STATUS
    PageId nextPageId, curPageId = rootPageId;
    short cur_pg_type = curSortedPage->get_type();
		status = MINIBASE_BM->unpinPage(rootPageId, 0, 0); CHECK_STATUS
    
    while(cur_pg_type != LEAF) {
				BTIndexPage* curIndexPage;
				status = MINIBASE_BM->pinPage(curPageId, (Page*&) curIndexPage, 0); CHECK_STATUS
        status = curIndexPage->get_page_no(key, key_type, nextPageId); CHECK_STATUS
        
        status = MINIBASE_BM->unpinPage(curPageId, 0, 0); CHECK_STATUS
        curPageId = nextPageId;
        
        status = MINIBASE_BM->pinPage(curPageId, (Page *&)curSortedPage, 0); CHECK_STATUS
        cur_pg_type = curSortedPage->get_type();
				status = MINIBASE_BM->unpinPage(curPageId, 0, 0);CHECK_STATUS 
    }

		status = MINIBASE_BM->pinPage(curPageId, (Page *&) curSortedPage, 0); CHECK_STATUS
    
    int i, cmp;
    Keytype *key_ = (Keytype*) malloc(sizeof(Keytype));
    Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
    
    for (i = 0; i < curSortedPage->slotCnt; ++i) {
        
        get_key_data(key_, data_, 
					(KeyDataEntry*) &curSortedPage->data[curSortedPage->slot[i].offset], curSortedPage->slot[i].length, LEAF);
        cmp = keyCompare(key, key_, key_type);
        
        if (cmp == 0) {
            data_->rid.pageNo = -1;
            data_->rid.slotNo = -1;
            int recLen;
            make_entry((KeyDataEntry*) &curSortedPage->data[curSortedPage->slot[i].offset], key_type, key, LEAF, *data_, &recLen);
						status = MINIBASE_BM->unpinPage(curPageId, 1, 0); CHECK_STATUS
            return OK;
        }
        
    }

		free(key_);
		free(data_);

		status = MINIBASE_BM->unpinPage(curPageId, 0, 0); CHECK_STATUS
    return FAIL;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
	Status d_status;
	return (IndexFileScan*) new BTreeFileScan(d_status, lo_key, hi_key, rootPageId, key_type);
}

int BTreeFile::keysize(){
  return key_size;
}
