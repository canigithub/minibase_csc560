/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

#ifndef CHECK_RETURN_STATUS
#define CHECK_RETURN_STATUS if (status != OK) {returnStatus = status; return;}
#endif


#ifndef CHECK_STATUS
#define CHECK_STATUS if(status != OK) {return status;}
#endif 

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */
 
 BTreeFileScan::BTreeFileScan(Status &returnStatus, const void *lo_k, const void *hi_k, PageId r_pgid, AttrType k_tp) {
    lo_key = (Keytype*) lo_k;
    hi_key = (Keytype*) hi_k;
    key_type = k_tp;
    scanIsDone = 0;
    scanJustStarted = 1;
    
    Status status;
    SortedPage *rootPage;
    status = MINIBASE_BM->pinPage(r_pgid, (Page*&) rootPage, 0); CHECK_RETURN_STATUS
    short pg_type = rootPage->get_type();
		status = MINIBASE_BM->unpinPage(r_pgid, 0, 0); CHECK_RETURN_STATUS;
    
    if (lo_key == NULL) {
        
        Keytype * d_key = (Keytype *) malloc(sizeof(Keytype));
        RID d_rid;
        PageId targetPageId, tempPageId;
        SortedPage *targetPage;
        
        if (pg_type == LEAF) {
						BTLeafPage* rootLeafPage;
						status = MINIBASE_BM->pinPage(r_pgid, (Page*&) rootLeafPage, 0); CHECK_RETURN_STATUS;
            status = rootLeafPage->get_first(curRid, d_key, d_rid);  CHECK_RETURN_STATUS;
            if (status == DONE) {
              scanIsDone = 1;
              status = MINIBASE_BM->unpinPage(r_pgid, 0, 1); CHECK_RETURN_STATUS
              returnStatus = DONE;
              return;
            }
            status = MINIBASE_BM->unpinPage(r_pgid, 0, 1); CHECK_RETURN_STATUS
        } else {
						BTIndexPage* rootIndexPage;
						status = MINIBASE_BM->pinPage(r_pgid, (Page*&) rootIndexPage, 0); CHECK_RETURN_STATUS
            status = rootIndexPage->get_first(d_rid, d_key, targetPageId); CHECK_RETURN_STATUS
						status = MINIBASE_BM->unpinPage(r_pgid, 0, 0); CHECK_RETURN_STATUS

            status = MINIBASE_BM->pinPage(targetPageId, (Page *&)targetPage, 0); CHECK_RETURN_STATUS
            short target_pg_type = targetPage->get_type();
						status = MINIBASE_BM->unpinPage(targetPageId, 0, 0);
            while(target_pg_type != LEAF) {
							BTIndexPage *targetIndexPage;
							status = MINIBASE_BM->pinPage(targetPageId, (Page*&) targetIndexPage, 0); CHECK_RETURN_STATUS
              status = targetIndexPage->get_first(d_rid, d_key, tempPageId); CHECK_RETURN_STATUS
              status = MINIBASE_BM->unpinPage(targetPageId, 0, 0); CHECK_RETURN_STATUS
              targetPageId = tempPageId;
              status = MINIBASE_BM->pinPage(targetPageId, (Page *&)targetPage, 0); CHECK_RETURN_STATUS
              target_pg_type = targetPage->get_type();
							status = MINIBASE_BM->unpinPage(targetPageId, 0, 0);
            }
            
						BTLeafPage *targetLeafPage;
						status = MINIBASE_BM->pinPage(targetPageId, (Page*&) targetLeafPage, 0); CHECK_RETURN_STATUS
            status = targetLeafPage->get_first(curRid, d_key, d_rid); CHECK_RETURN_STATUS
            
            /////// IF FIRST PAGE IS EMPTY
            
            status = MINIBASE_BM->unpinPage(targetPageId, 0, 0); CHECK_RETURN_STATUS  
        }
        free(d_key);
        
    } else {  // lo_key is not null
      
      //Keytype * d_key = (Keytype *) malloc(sizeof(Keytype));
      //RID d_rid;
      PageId targetPageId, tempPageId;
      //SortedPage *targetPage;
      
      if (pg_type == LEAF) { // that's the root page
        // status = (BTLeafPage *)rootPage->get_data_rid(lo_key, key_type, curRid);
          
          // iterate the slots

				BTLeafPage *rootLeafPage;
				status = MINIBASE_BM->pinPage(r_pgid, (Page*&) rootLeafPage, 0);
        int i = 1;
        int cmp = 0;
        void *key_ = NULL;
        Datatype *data_ = NULL;;
            
        for (; i < rootLeafPage->slotCnt; ++i) {
            get_key_data(key_, data_, (KeyDataEntry*) &rootLeafPage->data[rootLeafPage->slot[i].offset], rootLeafPage->slot[i].length, INDEX);
            cmp = keyCompare(lo_key, key_, key_type);
            if (cmp > 0) {
                continue;
            } else if (cmp < 0) {
                get_key_data(key_, data_, (KeyDataEntry*) &rootLeafPage->data[rootLeafPage->slot[i-1].offset], rootLeafPage->slot[i-1].length, INDEX);
                curRid = data_->rid;
                break;
            } else {
                curRid = data_->rid;
                break;
            }
        }
            
        if (i == rootLeafPage->slotCnt) {
          scanIsDone = 1;
          returnStatus = DONE;
          status = MINIBASE_BM->unpinPage(r_pgid, 0, 1); CHECK_RETURN_STATUS
          return;
        }
          
        // iterate the slots end
        status = MINIBASE_BM->unpinPage(r_pgid, 0, 1); CHECK_RETURN_STATUS
          
			// lo_key is not null, and root page is not index page
      } else {
					BTIndexPage *rootIndexPage;
					SortedPage *targetPage;
					BTIndexPage *targetIndexPage;
					status = MINIBASE_BM->pinPage(r_pgid, (Page*&) rootIndexPage, 0); CHECK_RETURN_STATUS
          status = rootIndexPage->get_page_no(lo_key, key_type, targetPageId); CHECK_RETURN_STATUS
					status = MINIBASE_BM->unpinPage(r_pgid, 0, 0); CHECK_RETURN_STATUS 

          status = MINIBASE_BM->pinPage(targetPageId, (Page *&)targetPage, 0); CHECK_RETURN_STATUS
          short target_pg_type = targetPage->get_type();
					status = MINIBASE_BM->unpinPage(targetPageId, 0, 0);

           while(target_pg_type != LEAF) {
						 status = MINIBASE_BM->pinPage(targetPageId, (Page*&) targetIndexPage, 0); CHECK_RETURN_STATUS
             status = targetIndexPage->get_page_no(lo_key, key_type, tempPageId); CHECK_RETURN_STATUS
             status = MINIBASE_BM->unpinPage(targetPageId, 0, 0); CHECK_RETURN_STATUS
             targetPageId = tempPageId;
             status = MINIBASE_BM->pinPage(targetPageId, (Page *&)targetPage, 0); CHECK_RETURN_STATUS
             target_pg_type = targetPage->get_type();
						 status = MINIBASE_BM->unpinPage(targetPageId, 0, 0);
           }
            
						BTLeafPage *targetLeafPage;
            
            int i = 1;
            int cmp = 0;
            Keytype *key_ = (Keytype*)malloc(sizeof(Keytype));
            Datatype *data_ = (Datatype*)malloc(sizeof(Datatype));
            
						status = MINIBASE_BM->pinPage(targetPageId, (Page*&) targetLeafPage, 0);

            for (; i < targetLeafPage->slotCnt; ++i) {
                get_key_data(key_, data_, (KeyDataEntry*) &targetLeafPage->data[targetLeafPage->slot[i].offset], targetLeafPage->slot[i].length, INDEX);
                cmp = keyCompare(lo_key, key_, key_type);
                if (cmp > 0) {
                    continue;
                } else if (cmp < 0) {
                    get_key_data(key_, data_, (KeyDataEntry*) &targetLeafPage->data[targetLeafPage->slot[i-1].offset], targetLeafPage->slot[i-1].length, INDEX);
                    curRid = data_->rid;
                    break;
                } else {
                    curRid = data_->rid;
                    break;
                }
            }
              
            if (i == targetLeafPage->slotCnt) {
                PageId nextPageId = targetLeafPage->getNextPage();
                status = MINIBASE_BM->unpinPage(targetPageId, 0, 0); CHECK_RETURN_STATUS
                PageId curPageId = nextPageId;
                BTLeafPage *curPage;
                status = MINIBASE_BM->pinPage(curPageId, (Page*&) curPage, 0); CHECK_RETURN_STATUS
                RID d_rid;
                Keytype *d_key = (Keytype*)malloc(sizeof(Keytype));
                status = curPage->get_first(curRid, d_key, d_rid);
                while (status == DONE) {
                  nextPageId = curPage->getNextPage();
                  if (nextPageId == -1) {
                    scanIsDone = 1;
                    returnStatus = DONE;
                    status = MINIBASE_BM->unpinPage(curPageId, 0, 0); CHECK_RETURN_STATUS
                    return;
                  }
                  status = MINIBASE_BM->unpinPage(curPageId, 0, 0); CHECK_RETURN_STATUS
                  curPageId = nextPageId;
                  status = MINIBASE_BM->pinPage(curPageId, (Page*&) curPage, 0); CHECK_RETURN_STATUS
                  status = curPage->get_first(curRid, d_key, d_rid); // DONE!
                }
								status = MINIBASE_BM->unpinPage(curPageId, 0, 0); CHECK_RETURN_STATUS
                returnStatus = OK;
                return;
            }
            
            status = MINIBASE_BM->unpinPage(targetPageId, 0, 0); CHECK_RETURN_STATUS
      }
    }
    
 };
 

BTreeFileScan::~BTreeFileScan()
{
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{
    Status status;
    if (scanIsDone) {
        return DONE;
    }
    
    // rid = curRid;
    
    PageId curPageId = curRid.pageNo;
    BTLeafPage *curPage;
    status = MINIBASE_BM->pinPage(curPageId, (Page *&)curPage, 0); CHECK_STATUS
    
    // assume rid is the dataRid
    if (scanJustStarted) {
         KeyDataEntry *psource = (KeyDataEntry*) &curPage->data[curPage->slot[curRid.slotNo].offset];
        Keytype *key = (Keytype *)malloc(sizeof(Keytype)); 
        Datatype *data = (Datatype *) malloc(sizeof(Datatype));
        int entryLen = curPage->slot[curRid.slotNo].length;
        get_key_data(key, data, psource, entryLen, LEAF);
        
				int cmp = 0;
				if(hi_key != NULL)
	        cmp = keyCompare(key, hi_key, key_type);
        
        if (hi_key != NULL && cmp > 0) {
          scanIsDone = 1;
          status = MINIBASE_BM->unpinPage(curPageId, 0, 1); CHECK_STATUS
          return DONE;
        }
        status = MINIBASE_BM->unpinPage(curPageId, 0, 1); CHECK_STATUS
        rid = data->rid;
        scanJustStarted = 0;
        memmove(keyptr, key, sizeof(Keytype));
        return OK;
    } 
    
    
    RID d_rid;
    status = curPage->get_next(curRid, keyptr, d_rid);
    while (status == DONE) {
      
        PageId nextPageId = curPage->getNextPage();
        if (nextPageId == -1) {
          scanIsDone = 1;
        	status = MINIBASE_BM->unpinPage(curPageId, 0, 1); CHECK_STATUS
          return DONE;
        } 
        status = MINIBASE_BM->unpinPage(curPageId, 0, 1); CHECK_STATUS
        curPageId = nextPageId;
        status = MINIBASE_BM->pinPage(curPageId, (Page*&) curPage, 0); CHECK_STATUS
        status = curPage->get_first(curRid, keyptr, rid);
    }
    status = MINIBASE_BM->unpinPage(curPageId, 0, 1); CHECK_STATUS
		if(hi_key != NULL) {
			int cmp = keyCompare(keyptr, hi_key, key_type);
			if(cmp > 0) {
				scanIsDone = 1;
				return DONE;
			}
		}
		rid = curRid;
    return OK;
}

Status BTreeFileScan::delete_current()
{
    Status status;
    
    BTLeafPage *curPage;
    status = MINIBASE_BM->pinPage(curRid.pageNo, (Page *&)curPage, 0); CHECK_STATUS
    
    KeyDataEntry *psource = (KeyDataEntry*) &curPage->data[curPage->slot[curRid.slotNo].offset];
    
    Keytype *key_ = (Keytype*) malloc(sizeof(Keytype));
    Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
    get_key_data(key_, data_, psource, curPage->slot[curRid.slotNo].length, LEAF);
    Datatype data;
    data.rid.pageNo = -1;
    data.rid.slotNo = -1;
    int recLen;
    make_entry(psource, key_type, key_, LEAF, data, &recLen);
    
    return OK;
}


int BTreeFileScan::keysize() 
{
  // put your code here
  return sizeof(key_type);
}
