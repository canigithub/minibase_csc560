#include "heapfile.h"

#define CHECK_RETURN_STATUS if (status != OK) {returnStatus = status; return;}
#define CHECK_STATUS if (status != OK) {return FAIL;}
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
HeapFile::HeapFile( const char *name, Status& returnStatus )

	// We assume the file already existed
	Status status = MINIBASE_DB->get_file_entry(name, firstDirPageId);
	// If not, ask the database to allocate a new page use it for a new file entry, and make it a directory page
	if(status != OK)  {								
		HFPage *firstDirPage = NULL;
		status = MINIBASE_DB->allocate_page(firstDirPageId, 1);             CHECK_RETURN_STATUS ;
		status = MINIBASE_DB->add_file_entry(name, firstDirPageId);         CHECK_RETURN_STATUS ;
		status = DATABASE_BM->pinPage(firstDirPageId, (Page*)firstDirPage);        CHECK_RETURN_STATUS ;
        firstDirPage->init(firstDirPageId);
		status = DATABASE_BM->unpinPage(firstDirPageId);                    CHECK_RETURN_STATUS ;
	}  /*else {				// otherwise, the file already existed; do we have to load information about it?
		Page* filePage = NULL;
		status = MINIDBASE_BM.pinPage(firstDirPageId, &filePage);   // do we need filename parameter?
		CHECK_STATUS ;
	} */

	fileName = strcpy(name);
	file_deleted = 0;
  
	returnStatus = OK;
   
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
   // don't delete the file if it wasn't temporary
	if(strcmp(filename, "")) return;
    Status status = deleteFile(); ASSERT_STATUS;
}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
    Status status;
	int num_records = 0;
	/* version 1 */
	PageId dirPageId = firstDirPageId;
	RID dataPageRid;
	HFPage* dirPage;
	DataPageInfo *dpinfop;
	int recLen
	while(dirPageId != -1) {
		status = MINIBASE_BM.pinPage(dirPageId, dirPage);  CHECK_STATUS ;
		status = dirPage->firstRecord(dataPageRid);	
		while(status == OK) {
			dirPage->getRecord(dataPageRid, (char *) dpinfop, recLen);
			assert(recLen == sizeof(DataPageInfo));
			num_records += dpinfo->recct;
			status = dirPage->nextRecord(dataPageRid);
			MINIBASE_BM.unpinPage(currDir);
		}
		if(status != DONE)
			return status;
		currDir = dirPage->getNextPage();

	/* version 2 */
	PageId currDirPageId = firstDirPageId;
    HFPage* currDirPage;
	RID currRid, nextRid;
	DataPageInfo *dpinfop;
    int recLen;
	while(currDirPageId != -1) {
        status = MINIBASE_BM.pinPage(currDirPageId, currDirPage);
        if (status != OK) return NO_SPACE;
		status = currDirPage->firstRecord(currRid);
		while(status == OK) {
			status = currDirPage->getRecord(currRid, (char *) dpinfop, recLen); ASSERT_STATUS;
            assert(recLen == sizeof(DataPageInfo));
			num_records += dpinfo->recct;
			status = currDirPage->nextRecord(currRid, nextRid);
            currRid = nextRid;
        }
        if(status != DONE) return status;
        status = MINIBASE_BM.unpinPage(currDir);
        if(status != OK) return status;
        currDirPageId = currDirPage->getNextPage();
	
	/* end of differences */

	}

  return num_records;
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
		int directoryEntryRecLen;
        Status status;

		PageId targetPageId = -1;
		int reachedEndOfDirectory = 0; // why need this variable?

		while(targetPage == -1 && !reachedEndOfDirectory) {
            status = MINIBASE_BM.pinPage(currDirPageId, currDirPage); CHECK_FAIL_STATUS;
            status = currDirPage->firstRecord(currRid); CHECK_FAIL_STATUS;	// start with first page in this directory
			while(status == OK) {
				status = currDirPage->getRecord(currRid, (char *) dpinfop, directoryEntryRecLen);
                CHECK_FAIL_STATUS;
				assert(directoryEntryRecLen == sizeof(DataPageInfo));
				if(dpinfop->availspace >= recLen + 2*sizeof(short)) {					// sizeof(slot_t), which we can't access here
					targetPageId = dpinfop->pageId;
                    // need to maintain DataPageInfo here
                    dpinfop->recct += 1;
                    dpinfop->availspace -= (recLen + 2*sizeof(short));
					break;  // found the page for the record
				}
				if(targetPageId != -1) // is this line redundant?
					break;
				status = currDirPage->nextRecord(currRid, nextRid);	// proceed to next page in this directory
                currRid = nextRid;
			}
            if(status != DONE) return status;

			 // status == DONE, so we reached the last record in a directory page	

			if(currDirPage->getNextPage() != -1) {			// advance to the next directory page
				nextDirPageId = currDirPage->getNextPage();
				MINIBASE_BM.unpinPage(currDirPageId);
				currDirPageId = nextDirPageId;
			} else {
                end_of_dirPage = currDirPageId;
                MINIBASE_BM.unpinPage(currDirPageId);
				break;
			}

		}
        if(targetPageId == -1) {
            DataPageInfo *newPageInfo = malloc(sizeof(DataPageInfo));
            status = newDataPage(newPageInfo); CHECK_FAIL_STATUS;
            newPageInfo->recct += 1;
            newPageInfo->availspace -= (recLen + 2*sizeof(short));
            targetPageId = newPageInfo->pageId;
            status = allocateDirSpace(newPageInfo, throwaway, throwaway2); CHECK_FAIL_STATUS;
        }
        HFPage *actualPage;
        status = MINIBASE_BM.pinPage(targetPageId, actualPage); CHECK_FAIL_STATUS;
        status = actualPage->insertRecord(recPtr, recLen, outRID); CHECK_FAIL_STATUS;

    return OK;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
    if (file_deleted) return FAIL;
    PageId targetPageId = rid.pageNo;
    PageId currDirPageId = firstDirPageId;
    HFPage* currDirPage = NULL, targetDataPage = NULL, prevDirPage = NULL;
    DataPageInfo *dpinfop = NULL;
    RID currRid, nextRid;
    Status status;
    char* dummy_ptr = NULL;
    int recLen;
    while (currDirPageId != -1) {
        status = MINIBASE_BM->pinPage(currDirPageId, (Page*)currDirPage); CHECK_FAIL_STATUS;
        status = currDirPage->firstRecord(currRid);
        while (status == OK) {
            status = currDirPage->getRecord(currRid, (char*)dpinfop, recLen); CHECK_FAIL_STATUS;
            assert(recLen == sizeof(DataPageInfo));
            if (dpinfop->pageId == targetPageId) {
                Status ret_status;  // return_status;
                status = MINIBASE_BM->pinPage(targetPageId, (Page*)targetDataPage); CHECK_FAIL_STATUS;
                int del_recLen = -1;
                targetDataPage->getRecord(rid, dummy_ptr, del_recLen);
                ret_status = targetDataPage->deleteRecord(rid);
                if (ret_status == OK) {
                    if (targetDataPage->empty()) { // if empty data page, then deallocate it
                        status = MINIBASE_DB->deallocate_page(targetPageId); CHECK_FAIL_STATUS;
                        status = currDirPage->deleteRecord(currRid); CHECK_FAIL_STATUS;
                        if (currDirPage->empty()) {
                            PageId prevDelDirPage = currDirPage->getPrevPage();
                            PageId nextDelDirPage = currDirPage->getNextPage();
                            if (prevDelDirPage == -1) {firstDirPage = nextDelDirPage; return ret_status;}
                            status = MINIBASE_BM->pinPage(prevDelDirPage, (Page*)prevDirPage); CHECK_FAIL_STATUS;
                            prevDirPage->setNextPage(nextDelDirPage);
                            status = MINIBASE_BM->unpinPage(prevDelDirPage); CHECK_FAIL_STATUS;
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
            status = currDirPage->nextRecord(currRID, nextRID);
            currRID = nextRID;
        }
        status = MINIBASE_BM->unpinPage(currDirPageId); CHECK_FAIL_STATUS;
        currDirPageId = currDirPage->getNextPage();
    }
    return FAIL;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
    PageId dirPageId, dataPageId;
    HFPage* dirPage, dataPage;
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
    HFPage* dirPage, dataPage;
    RID retRid;
    Status status = findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, retRid);
    if (dirPage == NULL || dataPage == NULL) return FAIL;
    return dataPage->getRecord(rid, recPtr, recLen);
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
  // fill in the body 
  return NULL;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    if (file_deleted) return FAIL; // is a delete file still existing?
    file_deleted = 1;
    HFPage *currDirPage = NULL;
    Status status = DATABASE_BM->pinPage(firstDirPageId, (Page*)currDirPage); CHECK_FAIL_STATUS;
    PageId currDirPageId = firstDirPageId;
    PageId nextDirPageId = currDirPage->getNextPage();
    PageId currDataPageId;
    RID currRid, nextRid;
    DataPageInfo *dpinfop = NULL;
    int recLen;
    
    while(currDirPageId != -1) {
        status = currDirPage->firstRecord(currRid);
        while (status == OK) {
            status = currDirPage->getRecord(currRid, (char *) dpinfop, recLen); ASSERT_STATUS;
            assert(recLen == sizeof(DataPageInfo));
            currDataPageId = dpinfop->pageId;
            MINIBASE_DB->deallocate_page(currDataPageId);
            status = currDirPage->nextRecord(currRid, nextRid);
            currRid = nextRid;
        }
        nextDirPageId = currDirPage->getNextPage();
        MINIBASE_DB->deallocate_page(currDirPageId);
        currDirPageId = nextDirPageId;
    }
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
		PageId pageId;
		HFPage *newPage;
		Status status;

        status = MINIBASE_DB.allocate_page(pageId, 1); CHECK_FAIL_STATUS;
        status = MINIBASE_BM.pinPage(pageId, (Page*)newPage); CHECK_FAIL_STATUS;
        newPage->init(pageId);
		dpinfo->pageId = pageId;
		dpinfo->recct = 0;
		dpinfo->availspace = newPage->available_space();
        status = MINIBASE_BM.unpinPage(pageId); CHECK_FAIL_STATUS;
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
    PageId rpDataPageId = rid.pageNo;
    PageId rpDirPageId = firstDirPageId;
    DataPageInfo *dpinfop = NULL;
    RID currDirRid, nextDirRid;
    Status status;
    char* dummy_ptr;
    int dummy_recLen;
    int dpinfo_recLen;
    while (rpDirPageId != -1) {
        status = MINIBASE_BM->pinPage(rpDirPageId, (Page*)rpdirpage); CHECK_FAIL_STATUS;
        status = rpdirpage->firstRecord(currDirRid);
        while (status == OK) {
            status = rpdirpage->getRecord(currDirRid, (char*)dpinfop, dpinfo_recLen); CHECK_FAIL_STATUS;
            assert(dpinfo_recLen == sizeof(DataPageInfo));
            if (dpinfop->pageId == rpDataPageId) {
                status = MINIBASE_BM->pinPage(rpDataPageId, (Page*)rpdatapage); CHECK_FAIL_STATUS;
                status = rpdatapage->getRecord(rid, dummy_ptr, dummy_recLen);
                if (status != OK) {rpdirpage = NULL; rpdatapage = NULL; return status;}
                rpDataPageRid.slotNo = rid.slotNo;
                rpDataPageRid.pageNo = rid.pageNo;
                return OK;
            } else {
                status = rpdirpage->nextRecord(currDirRid, nextDirRid);
                currDirRid = nextDirRid;
            }
        }
        rpDirPageId = rpdirpage->getNextPage();
        status = MINIBASE_BM->unpinPage(rpDirPageId); CHECK_FAIL_STATUS;
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
    HFPage* newDirPage;
    Status status;
    status = MINIBASE_DB.allocate_page(allocDirPageId, 1); CHECK_FAIL_STATUS;
    status = MINIBASE_BM.pinPage(allocDirPageId, newDirPage); CHECK_FAIL_STATUS;
    newDirPage->init(allocDirPageId);
    status = newDirPage->insertRecord((char*)dpinfop, sizeof(DataPageInfo), allocDataPageRid);
    CHECK_FAIL_STATUS;
    newDirPage->setPrevPage(end_of_dirPage);
    end_of_dirPage = allocDirPageId;
    return OK;
}

// *******************************************
