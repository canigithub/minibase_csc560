#include "heapfile.h"

#define CHECK_RETURN_STATUS if (status != OK) {returnStatus = status; return;}
#define CHECK_STATUS if (status != OK) {return status;}
#define ASSERT_STATUS assert(status == OK);


// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer", 
    "end of file encountered",
    "invalid update operation",
    "no space on page for record", 
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

PageId end_of_dirPage; /// remember to comment this line in the end

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus ) {

	// We assume the file already existed
	Status status = MINIBASE_DB->get_file_entry(name, firstDirPageId);
	// If not, ask the database to allocate a new page use it for a new file entry, and make it a directory page
	if(status != OK)  {								
		HFPage *firstDirPage = NULL;
		status = MINIBASE_DB->allocate_page(firstDirPageId, 1); CHECK_RETURN_STATUS
		status = MINIBASE_DB->add_file_entry(name, firstDirPageId); CHECK_RETURN_STATUS
		status = MINIBASE_BM->pinPage(firstDirPageId, (Page*&)firstDirPage);        
        CHECK_RETURN_STATUS
        firstDirPage->init(firstDirPageId);
		status = MINIBASE_BM->unpinPage(firstDirPageId, TRUE); CHECK_RETURN_STATUS
	}
	strcpy(fileName, name);
	// end_of_dirPage = firstDirPageId;
	file_deleted = 0;
  
	returnStatus = OK;
   
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
   // don't delete the file if it wasn't temporary
	if(strcmp(fileName, "")) return;
    Status status = deleteFile(); ASSERT_STATUS;
}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
    Status status;
	int num_records = 0;
	PageId currDirPageId = firstDirPageId, nextDirPageId;
    HFPage* currDirPage;
	RID currdpinfoRid, nextdpinfoRid;
	// DataPageInfo *dpinfop = (DataPageInfo*) malloc(sizeof(DataPageInfo));
    DataPageInfo *dpinfop;
    int dpinfoRecLen;
	while(currDirPageId != -1) {
        status = MINIBASE_BM->pinPage(currDirPageId, (Page*&) currDirPage); CHECK_STATUS
        // if (status != OK) return NO_SPACE;
		status = currDirPage->firstRecord(currdpinfoRid);
		while(status == OK) {
			status = currDirPage->returnRecord(currdpinfoRid, (char *&)dpinfop, dpinfoRecLen); CHECK_STATUS
            assert(dpinfoRecLen == sizeof(DataPageInfo));
			// if(status == DONE) break;
			num_records += dpinfop->recct;
			status = currDirPage->nextRecord(currdpinfoRid, nextdpinfoRid);
            currdpinfoRid = nextdpinfoRid;
        }
		nextDirPageId = currDirPage->getNextPage();
        status = MINIBASE_BM->unpinPage(currDirPageId);
  	    currDirPageId = nextDirPageId;
	}
	// free(dpinfop);
    return num_records;
}

void checkPins(int i) {
		if(MINIBASE_BM->getNumUnpinnedBuffers() != MINIBASE_BM->getNumBuffers())
			printf("Warning! at time %d, %d pages are pinned\n", 
				i, MINIBASE_BM->getNumBuffers() - MINIBASE_BM->getNumUnpinnedBuffers());
	}
// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
    // PageId throwaway; // I see no reason why we need this information
	// RID throwaway2;
    Status status;
    PageId dummyPageId;
    RID dummyRid;
    // assume heapfile exists and firstDirPageId is valid and firstDirPage exists
    PageId currDirPageId = firstDirPageId, nextDirPageId;
    RID currdpinfoRid, nextdpinfoRid;
    HFPage *currDirPage, *targetDataPage, *newDirPage;
    DataPageInfo *dpinfop;
    PageId targetDataPageId = -1;
    int dummyRecLen;
    
    while(currDirPageId != -1) {
        // cout << "dirPageId:" << currDirPageId << ' ';  
        status = MINIBASE_BM->pinPage(currDirPageId, (Page*&) currDirPage); CHECK_STATUS
        // cout << "dirPageSize:" << currDirPage->available_space() << "(" << currDirPageId << ")" << ' ';
        status = currDirPage->firstRecord(currdpinfoRid);   
        /* iterate over data pages in directory page */
        // int cnt = 0;
        while(status == OK){
            // cout << ++cnt << ' ';
            status = currDirPage->returnRecord(currdpinfoRid, (char*&)dpinfop, dummyRecLen); CHECK_STATUS
            assert(dummyRecLen == sizeof(DataPageInfo));
            
            /* if a data page with enough space is found */
            if(dpinfop->availspace >= (int)(recLen + 2*sizeof(short))) {
                // cout << "I data" << ' ';
                // cout << "dpinfo:" << dpinfop->availspace << ' ';
                targetDataPageId = dpinfop->pageId;
                status = MINIBASE_BM->pinPage(targetDataPageId, (Page*&)targetDataPage); CHECK_STATUS
                // cout << "dataPageSpace:" << targetDataPage->available_space() << ' ';
                status = targetDataPage->insertRecord(recPtr, recLen, outRid); CHECK_STATUS 
                // cout << "status:" << status << ' '; if (status != OK) return status; 
                // ASSERT_STATUS
                dpinfop->recct += 1;
                dpinfop->availspace -= int(recLen + 2*sizeof(short));
                assert(dpinfop->availspace == targetDataPage->available_space());
                // cout << "dpinfo2:" << dpinfop->availspace << ' ';
                // cout << "dataPageSpace2:" << targetDataPage->available_space() << ' ';
                status = MINIBASE_BM->unpinPage(targetDataPageId, TRUE); CHECK_STATUS
                status = MINIBASE_BM->unpinPage(currDirPageId, TRUE); CHECK_STATUS
                // cout << "ready to return OK" << ' ';
                return OK;
			} else {
				status = currDirPage->nextRecord(currdpinfoRid, nextdpinfoRid);
				currdpinfoRid = nextdpinfoRid;
            }
		}
        assert(status == DONE);
        // cout << "dirPageSize2:" << currDirPage->available_space() << "(" << currDirPageId << ")" << ' ';
		/* if currDirPage has enough space for a dpinfo record */ 
		if(currDirPage->available_space() >= (int)(sizeof(DataPageInfo) + 2*sizeof(short))) {
            // cout << "I dir&data" << ' ';
			DataPageInfo *newDataPageInfo = (DataPageInfo*) malloc(sizeof(DataPageInfo));
			status = newDataPage(newDataPageInfo); CHECK_STATUS
			targetDataPageId = newDataPageInfo->pageId;
			status = MINIBASE_BM->pinPage(targetDataPageId, (Page*&)targetDataPage); CHECK_STATUS
			status = targetDataPage->insertRecord(recPtr, recLen, outRid); ASSERT_STATUS; /// 
			newDataPageInfo->recct += 1;
			newDataPageInfo->availspace -= (recLen + 2*sizeof(short));
            assert(newDataPageInfo->availspace == targetDataPage->available_space());
			status = currDirPage->insertRecord((char*)newDataPageInfo, sizeof(DataPageInfo), dummyRid);
            ASSERT_STATUS;
			status = MINIBASE_BM->unpinPage(targetDataPageId, TRUE); CHECK_STATUS
			status = MINIBASE_BM->unpinPage(currDirPageId, TRUE); CHECK_STATUS
			return OK;
		}
			
		nextDirPageId = currDirPage->getNextPage();
		status = MINIBASE_BM->unpinPage(currDirPageId); CHECK_STATUS

        /* if all dirPages are full, we need to allocate a new dirPage */
		if(nextDirPageId == -1) {
            // deal with new data page
            // cout << "create dirPage" << ' ';
			DataPageInfo *newDataPageInfo = (DataPageInfo*) malloc(sizeof(DataPageInfo));
			status = newDataPage(newDataPageInfo); CHECK_STATUS
            targetDataPageId = newDataPageInfo->pageId;
            status = MINIBASE_BM->pinPage(targetDataPageId, (Page*&)targetDataPage); CHECK_STATUS
			status = targetDataPage->insertRecord(recPtr, recLen, outRid); ASSERT_STATUS; /// 
			newDataPageInfo->recct += 1;
			newDataPageInfo->availspace -= (recLen + 2*sizeof(short));
            assert(newDataPageInfo->availspace == targetDataPage->available_space());
            status = MINIBASE_BM->unpinPage(targetDataPageId, TRUE); CHECK_STATUS
            // deal with new directory page
			status = allocateDirSpace(newDataPageInfo, dummyPageId, dummyRid); CHECK_STATUS ///// check here later
			// status = MINIBASE_BM->pinPage(dummyPageId, (Page*&)newDirPage); CHECK_STATUS
			// status = newDirPage->insertRecord((char*)newDataPageInfo, (int)sizeof(DataPageInfo), dummyRid); 
            // CHECK_STATUS
			// status = MINIBASE_BM->unpinPage(dummyPageId, TRUE); CHECK_STATUS
			return OK;
		} else { // if nextDirPage exists
            currDirPageId = nextDirPageId;
        }
	}
	return FAIL;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
    if (file_deleted) return FAIL;
    PageId targetPageId = rid.pageNo;
    PageId currDirPageId = firstDirPageId;
    HFPage *currDirPage = NULL, *targetDataPage = NULL, *prevDirPage = NULL;
    DataPageInfo *dpinfop = NULL;
    RID currRid, nextRid;
    Status status;
    char* dummy_ptr = NULL;
    int recLen;
    while (currDirPageId != -1) {
        status = MINIBASE_BM->pinPage(currDirPageId, (Page*&)currDirPage); CHECK_STATUS
        status = currDirPage->firstRecord(currRid);
        while (status == OK) {
            status = currDirPage->getRecord(currRid, (char*)dpinfop, recLen); CHECK_STATUS
            assert(recLen == sizeof(DataPageInfo));
            if (dpinfop->pageId == targetPageId) {
                Status ret_status;  // return_status;
                status = MINIBASE_BM->pinPage(targetPageId, (Page*&)targetDataPage); CHECK_STATUS
                int del_recLen = -1;
                targetDataPage->getRecord(rid, dummy_ptr, del_recLen);
                ret_status = targetDataPage->deleteRecord(rid);
                if (ret_status == OK) {
                    if (targetDataPage->empty()) { // if empty data page, then deallocate it
                        status = MINIBASE_DB->deallocate_page(targetPageId); CHECK_STATUS
                        status = currDirPage->deleteRecord(currRid); CHECK_STATUS
                        if (currDirPage->empty()) {
                            PageId prevDelDirPage = currDirPage->getPrevPage();
                            PageId nextDelDirPage = currDirPage->getNextPage();
                            if (currDirPageId == end_of_dirPage) {
                                end_of_dirPage = prevDelDirPage;
                            }
                            if (prevDelDirPage == -1) {firstDirPageId = nextDelDirPage; return ret_status;}
                            status = MINIBASE_BM->pinPage(prevDelDirPage, (Page*&)prevDirPage); CHECK_STATUS
                            prevDirPage->setNextPage(nextDelDirPage);
                            status = MINIBASE_BM->unpinPage(prevDelDirPage); CHECK_STATUS
                            MINIBASE_DB->deallocate_page(currDirPageId);
                        }
                    }
                    else {
                        dpinfop->availspace += (del_recLen + 2*sizeof(short));
                        dpinfop->recct -= 1;
                    }
                }
                return ret_status;
            }
            status = currDirPage->nextRecord(currRid, nextRid);
            currRid = nextRid;
        }
        status = MINIBASE_BM->unpinPage(currDirPageId); CHECK_STATUS
        currDirPageId = currDirPage->getNextPage();
    }
    return FAIL;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
    PageId dirPageId, dataPageId;
    HFPage *dirPage, *dataPage;
    RID retRid;
    int dummy_recLen;
    char* dummy_ptr;
    Status status = findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, retRid);
    if (dirPage == NULL || dataPage == NULL) return FAIL;
    status = dataPage->returnRecord(rid, dummy_ptr, dummy_recLen);
    if (status != OK) return status;
    if (dummy_recLen != recLen) return FAIL;
    memcpy(dummy_ptr, recPtr, recLen);
    return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
    PageId dirPageId, dataPageId;
    HFPage *dirPage, *dataPage;
    RID retRid;
    Status status = findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, retRid);
		CHECK_STATUS
    if (dirPage == NULL || dataPage == NULL) return FAIL;
    return dataPage->getRecord(rid, recPtr, recLen);
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
    Scan *s = new Scan(this,status);
	// printf("opened the scan, inside heapfile\n");
    return s;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    if (file_deleted) return FAIL; // is a delete file still existing?
    file_deleted = 1;
    HFPage *currDirPage = NULL;
    Status status = MINIBASE_BM->pinPage(firstDirPageId, (Page*&)currDirPage); CHECK_STATUS
    PageId currDirPageId = firstDirPageId;
    PageId nextDirPageId = currDirPage->getNextPage();
    PageId currDataPageId;
    RID currRid, nextRid;
    DataPageInfo *dpinfop = NULL;
    int recLen;
    
    while(currDirPageId != -1) {
        status = currDirPage->firstRecord(currRid);
        while (status == OK) {
            status = currDirPage->getRecord(currRid, (char *) dpinfop, recLen); CHECK_STATUS
            assert(recLen == sizeof(DataPageInfo));
            currDataPageId = dpinfop->pageId;
            status = MINIBASE_DB->deallocate_page(currDataPageId); CHECK_STATUS
            status = currDirPage->nextRecord(currRid, nextRid);
            currRid = nextRid;
        }
        nextDirPageId = currDirPage->getNextPage();
        status = MINIBASE_DB->deallocate_page(currDirPageId); CHECK_STATUS
        currDirPageId = nextDirPageId;
    }
		return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    // cout << "now alloc new dataPage" << ' ';
    Status status;
	PageId newDataPageId;
	HFPage *newDataPage;
	status = MINIBASE_DB->allocate_page(newDataPageId, 1); CHECK_STATUS
    status = MINIBASE_BM->pinPage(newDataPageId, (Page*&)newDataPage); CHECK_STATUS
    newDataPage->init(newDataPageId);
	dpinfop->pageId = newDataPageId;
	dpinfop->recct = 0;
	dpinfop->availspace = newDataPage->available_space();
    status = MINIBASE_BM->unpinPage(newDataPageId, TRUE); CHECK_STATUS
    return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage,
                    RID &rpDataPageRid) // why needs this?
{
    rpDataPageId = rid.pageNo;
    rpDirPageId = firstDirPageId;
    DataPageInfo *dpinfop = NULL;
    RID currDirRid, nextDirRid;
    Status status;
    char* dummy_ptr = NULL;
    int dummy_recLen;
    int dpinfo_recLen;
    while (rpDirPageId != -1) {
        status = MINIBASE_BM->pinPage(rpDirPageId, (Page*&)rpdirpage); CHECK_STATUS
        status = rpdirpage->firstRecord(currDirRid);
        while (status == OK) {
            status = rpdirpage->getRecord(currDirRid, (char*)dpinfop, dpinfo_recLen); CHECK_STATUS
            assert(dpinfo_recLen == sizeof(DataPageInfo));
            if (dpinfop->pageId == rpDataPageId) {
                status = MINIBASE_BM->pinPage(rpDataPageId, (Page*&)rpdatapage); CHECK_STATUS
                status = rpdatapage->returnRecord(rid, dummy_ptr, dummy_recLen);
                if (status != OK) {rpdirpage = NULL; rpdatapage = NULL; return status;}
								rpDataPageRid = currDirRid;
                return OK;
            } else {
                status = rpdirpage->nextRecord(currDirRid, nextDirRid);
                currDirRid = nextDirRid;
            }
        }
        rpDirPageId = rpdirpage->getNextPage();
        status = MINIBASE_BM->unpinPage(rpDirPageId); CHECK_STATUS
    }
    rpdirpage = NULL; rpdatapage = NULL;
    return FAIL;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // cout << "now alloc new dirPage" << ' ';
    Status status;
    HFPage *allocDirPage, *dummyDirPage;
    PageId nextDirPageId;
    status = MINIBASE_DB->allocate_page(allocDirPageId, 1); CHECK_STATUS
    status = MINIBASE_BM->pinPage(allocDirPageId, (Page*&)allocDirPage); CHECK_STATUS
    allocDirPage->init(allocDirPageId);
    // cout << "size of new dirPage:" << allocDirPage->available_space() << ' ';
    // find the last dirPage
    PageId lastDirPageId = firstDirPageId;
    while(lastDirPageId != -1) {
        status = MINIBASE_BM->pinPage(lastDirPageId, (Page*&)dummyDirPage); CHECK_STATUS
        if (dummyDirPage->getNextPage() == -1) { // now it is the last directory page
            dummyDirPage->setNextPage(allocDirPageId); // set allocDirPageId as 'next' of last dirPage
            status = MINIBASE_BM->unpinPage(lastDirPageId, TRUE); CHECK_STATUS
            break;
        } else {
            nextDirPageId = dummyDirPage->getNextPage();
            status = MINIBASE_BM->unpinPage(lastDirPageId); CHECK_STATUS
            lastDirPageId = nextDirPageId;
        }
    }
    assert(lastDirPageId != -1);
    allocDirPage->setPrevPage(lastDirPageId);
    status = allocDirPage->insertRecord((char*)dpinfop, sizeof(DataPageInfo), allocDataPageRid); CHECK_STATUS
    // cout << "size of new dirPage2:" << allocDirPage->available_space() << ' ';
	status = MINIBASE_BM->unpinPage(allocDirPageId, TRUE); CHECK_STATUS
    // end_of_dirPage = allocDirPageId;
    return OK;
}
