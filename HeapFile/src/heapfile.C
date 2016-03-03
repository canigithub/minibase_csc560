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

PageId end_of_dirPage;

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus ) {

	// We assume the file already existed
	Status status = MINIBASE_DB->get_file_entry(name, firstDirPageId);
	// If not, ask the database to allocate a new page use it for a new file entry, and make it a directory page
	if(status != OK)  {								
		HFPage *firstDirPage = NULL;
		status = MINIBASE_DB->allocate_page(firstDirPageId, 1);             CHECK_RETURN_STATUS ;
		status = MINIBASE_DB->add_file_entry(name, firstDirPageId);         CHECK_RETURN_STATUS ;
		status = MINIBASE_BM->pinPage((int)firstDirPageId, (Page*&)firstDirPage);        CHECK_RETURN_STATUS ;
        firstDirPage->init(firstDirPageId);
		status = MINIBASE_BM->unpinPage(firstDirPageId);                    CHECK_RETURN_STATUS ;
	}  /*else {				// otherwise, the file already existed; do we have to load information about it?
		Page* filePage = NULL;
		status = MINIDBASE_BM->pinPage(firstDirPageId, &filePage);   // do we need filename parameter?
		CHECK_STATUS ;
	} */

	strcpy(fileName, name);
	end_of_dirPage = firstDirPageId;
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
	PageId currDirPageId = firstDirPageId;
	PageId nextDirPageId;
  HFPage* currDirPage;
	RID currRid, nextRid;
	DataPageInfo *dpinfop = (DataPageInfo*) malloc(sizeof(DataPageInfo));
  int recLen;
	while(currDirPageId != -1) {
  	status = MINIBASE_BM->pinPage(currDirPageId, (Page*&) currDirPage);
    if (status != OK) return NO_SPACE;
		status = currDirPage->firstRecord(currRid);
		while(status == OK) {
			status = currDirPage->getRecord(currRid, (char *) dpinfop, recLen);
      assert(recLen == sizeof(DataPageInfo));
			if(status == DONE)
				break;
			num_records += dpinfop->recct;
			status = currDirPage->nextRecord(currRid, nextRid);
      currRid = nextRid;
    }
		nextDirPageId = currDirPage->getNextPage();
    status = MINIBASE_BM->unpinPage(currDirPageId);
  	currDirPageId = nextDirPageId;
	}
	free(dpinfop);
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
		PageId throwaway; // I see no reason why we need this information
		RID throwaway2;
		PageId currDirPageId = firstDirPageId, nextDirPageId;
		RID currRid, nextRid;
		HFPage *currDirPage;
		DataPageInfo *dpinfop;
		HFPage *dataPage;
    Status status;
		PageId targetPageId = -1;
		int garbageRecLen;


		while(currDirPageId != -1) {
    	status = MINIBASE_BM->pinPage(currDirPageId, (Page*&) currDirPage); CHECK_STATUS;															// PIN currDirPageId
			status = currDirPage->firstRecord(currRid); 
			// iterate over data pages in directory page
			while(status == OK){
				status = currDirPage->returnRecord(currRid, (char*&) dpinfop, garbageRecLen); CHECK_STATUS;
				assert(garbageRecLen == sizeof(DataPageInfo));

				// we can insert into a page in the current directory page
				HFPage *testDataPage;
				MINIBASE_BM->pinPage(dpinfop->pageId, (Page*&) testDataPage);
				//printf("heapfile says %d while page says %d\n", dpinfop->availspace, testDataPage->available_space());
				MINIBASE_BM->unpinPage(dpinfop->pageId);
				if(dpinfop->availspace >= recLen + (int)(2*sizeof(short))) {
					targetPageId = dpinfop->pageId;
					status = MINIBASE_BM->pinPage(targetPageId, (Page*&)dataPage);CHECK_STATUS;
					status = dataPage->insertRecord(recPtr, recLen, outRid); //CHECK_STATUS;
					dpinfop->recct += 1;
					dpinfop->availspace -= int(recLen + 2*sizeof(short));
					status = MINIBASE_BM->unpinPage(currDirPageId, TRUE); CHECK_STATUS;
					status = MINIBASE_BM->unpinPage(targetPageId, TRUE); CHECK_STATUS;

					return OK;
				}
				status = currDirPage->nextRecord(currRid, nextRid);
				currRid = nextRid;
			}

			// we can insert a new data page into the directory 
			if(currDirPage->available_space() >= (int)(sizeof(DataPageInfo) + 2*sizeof(short))) {
				DataPageInfo *newDataPageInfo = (DataPageInfo*) malloc(sizeof(DataPageInfo));
				status = newDataPage(newDataPageInfo);
				targetPageId = newDataPageInfo->pageId;
				status = MINIBASE_BM->pinPage(targetPageId, (Page*&) dataPage);
				dataPage->insertRecord(recPtr, recLen, outRid);
				newDataPageInfo->recct += 1;
				newDataPageInfo->availspace -= (recLen + 2*sizeof(short));
				RID junk;
				currDirPage->insertRecord((char*) newDataPageInfo, sizeof(DataPageInfo), junk);
				status = MINIBASE_BM->unpinPage(targetPageId, TRUE); CHECK_STATUS;
				status = MINIBASE_BM->unpinPage(currDirPageId, TRUE); CHECK_STATUS;
				return OK;
			}
			
			// we need to add a new directory page
			nextDirPageId = currDirPage->getNextPage();
			status = MINIBASE_BM->unpinPage(currDirPageId); CHECK_STATUS;

			if(nextDirPageId == -1) {
				DataPageInfo *newPageInfo = (DataPageInfo*) malloc(sizeof(DataPageInfo));
				status = newDataPage(newPageInfo); CHECK_STATUS;
				newPageInfo->recct += 1;
				newPageInfo->availspace -= (recLen + 2*sizeof(short));
				targetPageId = newPageInfo->pageId;
				status = allocateDirSpace(newPageInfo, throwaway, throwaway2); CHECK_STATUS;
				HFPage *actualPage = (HFPage *)malloc(sizeof(HFPage));
				status = MINIBASE_BM->pinPage(targetPageId, (Page*&) actualPage); CHECK_STATUS;
				status = actualPage->insertRecord(recPtr, recLen, outRid); CHECK_STATUS;
				status = MINIBASE_BM->unpinPage(targetPageId, TRUE); CHECK_STATUS;
				return OK;
			}
			currDirPageId = nextDirPageId;
		}
		return FAIL;

		/*checkPins(99);
    if(targetPageId == -1) {
      DataPageInfo *newPageInfo = (DataPageInfo*) malloc(sizeof(DataPageInfo));
      status = newDataPage(newPageInfo); CHECK_STATUS;
			printf("Passed status check #6.5\n");
      newPageInfo->recct += 1;
      newPageInfo->availspace -= (recLen + 2*sizeof(short));
      targetPageId = newPageInfo->pageId;
      status = allocateDirSpace(newPageInfo, throwaway, throwaway2); CHECK_STATUS;
			printf("Passed status check #7\n");
    }

		checkPins(8);
      HFPage *actualPage;
      status = MINIBASE_BM->pinPage(targetPageId, (Page*&)actualPage); CHECK_STATUS;
			printf("Passed status check #8\n");
      status = actualPage->insertRecord(recPtr, recLen, outRid); CHECK_STATUS;
			status = MINIBASE_BM->unpinPage(targetPageId); CHECK_STATUS;
			checkPins(9);

				printf("Passed status check #9\n");
    return OK; */
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
        status = MINIBASE_BM->pinPage(currDirPageId, (Page*&)currDirPage); CHECK_STATUS;
        status = currDirPage->firstRecord(currRid);
        while (status == OK) {
            status = currDirPage->getRecord(currRid, (char*)dpinfop, recLen); CHECK_STATUS;
            assert(recLen == sizeof(DataPageInfo));
            if (dpinfop->pageId == targetPageId) {
                Status ret_status;  // return_status;
                status = MINIBASE_BM->pinPage(targetPageId, (Page*&)targetDataPage); CHECK_STATUS;
                int del_recLen = -1;
                targetDataPage->getRecord(rid, dummy_ptr, del_recLen);
                ret_status = targetDataPage->deleteRecord(rid);
                if (ret_status == OK) {
                    if (targetDataPage->empty()) { // if empty data page, then deallocate it
                        status = MINIBASE_DB->deallocate_page(targetPageId); CHECK_STATUS;
                        status = currDirPage->deleteRecord(currRid); CHECK_STATUS;
                        if (currDirPage->empty()) {
                            PageId prevDelDirPage = currDirPage->getPrevPage();
                            PageId nextDelDirPage = currDirPage->getNextPage();
                            if (currDirPageId == end_of_dirPage) {
                                end_of_dirPage = prevDelDirPage;
                            }
                            if (prevDelDirPage == -1) {firstDirPageId = nextDelDirPage; return ret_status;}
                            status = MINIBASE_BM->pinPage(prevDelDirPage, (Page*&)prevDirPage); CHECK_STATUS;
                            prevDirPage->setNextPage(nextDelDirPage);
                            status = MINIBASE_BM->unpinPage(prevDelDirPage); CHECK_STATUS;
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
        status = MINIBASE_BM->unpinPage(currDirPageId); CHECK_STATUS;
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
		CHECK_STATUS;
    if (dirPage == NULL || dataPage == NULL) return FAIL;
    return dataPage->getRecord(rid, recPtr, recLen);
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
    Scan *s = new Scan(this,status);
		printf("opened the scan, inside heapfile\n");
    return s;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    if (file_deleted) return FAIL; // is a delete file still existing?
    file_deleted = 1;
    HFPage *currDirPage = NULL;
    Status status = MINIBASE_BM->pinPage(firstDirPageId, (Page*&)currDirPage); CHECK_STATUS;
    PageId currDirPageId = firstDirPageId;
    PageId nextDirPageId = currDirPage->getNextPage();
    PageId currDataPageId;
    RID currRid, nextRid;
    DataPageInfo *dpinfop = NULL;
    int recLen;
    
    while(currDirPageId != -1) {
        status = currDirPage->firstRecord(currRid);
        while (status == OK) {
            status = currDirPage->getRecord(currRid, (char *) dpinfop, recLen); CHECK_STATUS;
            assert(recLen == sizeof(DataPageInfo));
            currDataPageId = dpinfop->pageId;
            status = MINIBASE_DB->deallocate_page(currDataPageId); CHECK_STATUS;
            status = currDirPage->nextRecord(currRid, nextRid);
            currRid = nextRid;
        }
        nextDirPageId = currDirPage->getNextPage();
        status = MINIBASE_DB->deallocate_page(currDirPageId); CHECK_STATUS;
        currDirPageId = nextDirPageId;
    }
		return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
		PageId pageId;
		HFPage *newPage;
		Status status;

        status = MINIBASE_DB->allocate_page(pageId, 1); CHECK_STATUS;
        status = MINIBASE_BM->pinPage(pageId, (Page*&)newPage); CHECK_STATUS;
        newPage->init(pageId);
		dpinfop->pageId = pageId;
		dpinfop->recct = 0;
		dpinfop->availspace = newPage->available_space();
        status = MINIBASE_BM->unpinPage(pageId); CHECK_STATUS;
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
        status = MINIBASE_BM->pinPage(rpDirPageId, (Page*&)rpdirpage); CHECK_STATUS;
        status = rpdirpage->firstRecord(currDirRid);
        while (status == OK) {
            status = rpdirpage->getRecord(currDirRid, (char*)dpinfop, dpinfo_recLen); CHECK_STATUS;
            assert(dpinfo_recLen == sizeof(DataPageInfo));
            if (dpinfop->pageId == rpDataPageId) {
                status = MINIBASE_BM->pinPage(rpDataPageId, (Page*&)rpdatapage); CHECK_STATUS;
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
        status = MINIBASE_BM->unpinPage(rpDirPageId); CHECK_STATUS;
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
    HFPage* newDirPage, *oldDirPage;
    Status status;
    status = MINIBASE_DB->allocate_page(allocDirPageId, 1); CHECK_STATUS;
    status = MINIBASE_BM->pinPage(allocDirPageId, (Page*&) newDirPage); CHECK_STATUS;
    newDirPage->init(allocDirPageId);
    status = newDirPage->insertRecord((char*)dpinfop, sizeof(DataPageInfo), allocDataPageRid);
    CHECK_STATUS;
    newDirPage->setPrevPage(end_of_dirPage);
		status = MINIBASE_BM->unpinPage(allocDirPageId, TRUE); CHECK_STATUS;
		status = MINIBASE_BM->pinPage(end_of_dirPage, (Page*&) oldDirPage);
		oldDirPage->setNextPage(allocDirPageId);
		status = MINIBASE_BM->unpinPage(end_of_dirPage, TRUE); CHECK_STATUS;

    end_of_dirPage = allocDirPageId;
    return OK;
}
