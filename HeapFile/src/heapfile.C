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

// PageId end_of_dirPage; /// remember to comment this line in the end

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus ) {
    
    // cout << ">>> enter hf ctor." << endl;
	// We assume the file already existed
	Status status = MINIBASE_DB->get_file_entry(name, firstDirPageId);
    // cout << ">>> get_file_entry status = " << status << " firstDirPageId = " << firstDirPageId << endl;
    fileName = (char*) malloc(MAX_NAME * sizeof(char));
    strcpy(fileName, name);
        
	/* If not, ask the database to allocate a new page use it for a new file entry 
       and make it a directory page */
	if(status != OK)  {					
		HFPage *firstDirPage = NULL;
		status = MINIBASE_DB->allocate_page(firstDirPageId, 1); CHECK_RETURN_STATUS
        // cout << "@ after ctor.alloc_page" << ',' << std::flush;
        // cout << "firstDirPageId = " << firstDirPageId << ',' << std::flush;
		status = MINIBASE_DB->add_file_entry(name, firstDirPageId); CHECK_RETURN_STATUS
        // cout << "@ after ctor.add_entry" << ',' << std::flush;
		status = MINIBASE_BM->pinPage(firstDirPageId, (Page*&)firstDirPage); CHECK_RETURN_STATUS
        // cout << "@ after ctor.pinPage" << ',' << std::flush;
        firstDirPage->init(firstDirPageId);
        // cout << "@ after ctor.init" << ',' << std::flush;
        // cout << "fileNameSize = " << sizeof(fileName) << " name = " << name << endl << std::flush;
        // cout << "fileName = " << fileName << ' ' << &name << std::flush;
        // cout << "@ after ctor.strcpy" << endl << std::flush;
		status = MINIBASE_BM->unpinPage(firstDirPageId, TRUE); CHECK_RETURN_STATUS
        // cout << "@ after ctor.unpinPage" << endl << std::flush;
        
	}
	// cout << ">>> @ after if statement" << endl << std::flush;
    
	// end_of_dirPage = firstDirPageId;
	file_deleted = 0;
	returnStatus = OK;
    // cout << ">>> @ end of heapfile constructor" << endl << std::flush;
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
		Status status;
   // don't delete the file if it wasn't temporary
//    cout << ">>> enter dtor" << endl;
//    cout << "fileName = " << fileName << endl;
    if(!strcmp(fileName, "")) { 
        status = deleteFile(); 
				if(status != OK)	
					minibase_errors.show_errors();
    }
    free(fileName);
//    cout << ">>> enter the destructor." << endl;
    
    // cout << ">>> at the end of destructor." << endl;
    
    // cout << ">>> leave dtor" << endl;
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

// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
    // PageId throwaway; // I see no reason why we need this information
	// RID throwaway2;
    Status status;
    
    /* if recLen is longer than the size of an empty hfpage, log error */
    if ((int)(recLen + 2*sizeof(short)) > 1004) { // 1004 is the free space of an empty hfpage 
        minibase_errors.add_error(HEAPFILE, hfErrMsgs[4]);
        return HEAPFILE;
    }
    
    PageId dummyPageId;
    RID dummyRid;
    // assume heapfile exists and firstDirPageId is valid and firstDirPage exists
    PageId currDirPageId = firstDirPageId, nextDirPageId;
    RID currdpinfoRid, nextdpinfoRid;
    HFPage *currDirPage, *targetDataPage;
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
			status = targetDataPage->insertRecord(recPtr, recLen, outRid); CHECK_STATUS 
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
            // cout << "> create dirPage" << endl;;
			DataPageInfo *newDataPageInfo = (DataPageInfo*) malloc(sizeof(DataPageInfo));
			status = newDataPage(newDataPageInfo); CHECK_STATUS
            targetDataPageId = newDataPageInfo->pageId;
            status = MINIBASE_BM->pinPage(targetDataPageId, (Page*&)targetDataPage); CHECK_STATUS
			status = targetDataPage->insertRecord(recPtr, recLen, outRid); ASSERT_STATUS; 
			newDataPageInfo->recct += 1;
			newDataPageInfo->availspace -= (recLen + 2*sizeof(short));
            assert(newDataPageInfo->availspace == targetDataPage->available_space());
            status = MINIBASE_BM->unpinPage(targetDataPageId, TRUE); CHECK_STATUS
            // deal with new directory page
			status = allocateDirSpace(newDataPageInfo, dummyPageId, dummyRid); CHECK_STATUS
            // cout << "dummyPageId = " << dummyPageId << endl;
            
            /// test dirPage chain start
            // PageId testId = dummyPageId;
            // cout << "> file_1: " << testId << ", ";
            // HFPage *testPage;
            // status = MINIBASE_BM->pinPage(testId, (Page *&)testPage); if (status != OK) return status;
            // cout << "next:" << testPage->getNextPage() << " prev:" << testPage->getPrevPage() << endl;
            // status = MINIBASE_BM->unpinPage(testId); if (status != OK) return status;
            
            // HFPage *firstPage;
            // PageId testId2 = firstDirPageId, testId3;
            // cout << "firstDirPageId = " << firstDirPageId << endl;
            // cout << "> file_1(2): ";
            // while (testId2 != -1) {
            //     cout << testId2 << ", ";
            //     status = MINIBASE_BM->pinPage(testId2, (Page *&)firstPage); if (status != OK) return status;
            //     testId3 = firstPage->getNextPage();
            //     status = MINIBASE_BM->unpinPage(testId2); if (status != OK) return status;
            //     testId2 = testId3;
            // }
            
            /// end
             
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
    if (file_deleted) {
			return FAIL; 
		}
    PageId dirPageId, dataPageId;
    HFPage *dirPage, *dataPage, *prevDirPage, *nextDirPage;
    RID dataPageRid;
    DataPageInfo *dpinfop;
    int recLen, dataRecLen;
    char *dummyPtr;
    Status status;
    status = findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, dataPageRid); CHECK_STATUS
    if (dirPage == NULL || dataPage == NULL) return FAIL;
    
    /* if record is found*/
    status = dataPage->returnRecord(rid, dummyPtr, dataRecLen); CHECK_STATUS 
    status = dataPage->deleteRecord(rid); CHECK_STATUS
    status = dirPage->returnRecord(dataPageRid, (char *&)dpinfop, recLen); CHECK_STATUS
    dpinfop->recct += 1;
    dpinfop->availspace += (int)(dataRecLen + 2*sizeof(short));
    // cout << '(' << dpinfop->availspace << ',' << dataPage->available_space() <<')' << endl;
    assert(dpinfop->availspace == dataPage->available_space());
    
    /* if dataPage is not empty after deletion */
    if (!dataPage->empty()) {
        status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
        status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS            
        return OK;
    }
    
    status = MINIBASE_DB->deallocate_page(dataPageId); CHECK_STATUS
    status = dirPage->deleteRecord(dataPageRid); CHECK_STATUS
    
    /* if dataPage is empty but dirPage is not empty after deletion */
    if (!dirPage->empty()) {
        status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
        status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS            
        return OK;
    }
    
    /* if both dataPage and dirPage is empty after deletion */
    PageId prevDirPageId, nextDirPageId;
    prevDirPageId = dirPage->getPrevPage();
    nextDirPageId = dirPage->getNextPage();

    if (prevDirPageId == -1 && nextDirPageId == -1) {
			// this is the only directory page... hold onto it
	    status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
	    status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS            
	    return OK;

		// otherwise, remove the directory page from the linked list
    } else  if (prevDirPageId == -1 && nextDirPageId != -1) {
        status = MINIBASE_BM->pinPage(nextDirPageId, (Page*&) nextDirPage); CHECK_STATUS
        nextDirPage->setPrevPage(-1);
        status = MINIBASE_BM->unpinPage(nextDirPageId); CHECK_STATUS
        firstDirPageId = nextDirPageId;
    } else if (prevDirPageId != -1 && nextDirPageId == -1) {
        status = MINIBASE_BM->pinPage(prevDirPageId, (Page*&) prevDirPage); CHECK_STATUS
        prevDirPage->setNextPage(-1);
        status = MINIBASE_BM->unpinPage(prevDirPageId); CHECK_STATUS
    } else {
        status = MINIBASE_BM->pinPage(prevDirPageId, (Page*&) prevDirPage); CHECK_STATUS
        status = MINIBASE_BM->pinPage(nextDirPageId, (Page*&) nextDirPage); CHECK_STATUS
        prevDirPage->setNextPage(nextDirPageId);
        nextDirPage->setPrevPage(prevDirPageId);
        status = MINIBASE_BM->unpinPage(prevDirPageId); CHECK_STATUS
        status = MINIBASE_BM->unpinPage(nextDirPageId); CHECK_STATUS
    }
    
    status = MINIBASE_DB->deallocate_page(dirPageId); CHECK_STATUS
    status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
    status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS            
    return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
    PageId dirPageId, dataPageId;
    HFPage *dirPage, *dataPage;
    RID dummyRid;
    int len;
    char* dummyPtr;
    Status status;
    status = findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, dummyRid);
    if (dirPage == NULL || dataPage == NULL) return FAIL;
    status = dataPage->returnRecord(rid, dummyPtr, len); CHECK_STATUS
    /* if the updated record length is not equal to the original, return FAIL */
    if (len != recLen) {
        // hfTable(HEAPFILE, hfErrMsgs[3]);
        minibase_errors.add_error(HEAPFILE, hfErrMsgs[3]);
        return HEAPFILE;
    }
    memcpy(dummyPtr, recPtr, len);
    status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
    status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS        
    return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
    PageId dirPageId, dataPageId;
    HFPage *dirPage, *dataPage;
    RID retRid;
    Status status;
    status = findDataPage(rid, dirPageId, dirPage, dataPageId, dataPage, retRid); CHECK_STATUS
    if (dirPage == NULL || dataPage == NULL) return FAIL;
    Status ret_status =  dataPage->getRecord(rid, recPtr, recLen);
    status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
    status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS        
    return ret_status;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
    Scan *s = new Scan(this,status);
    return s;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{   
    // cout << "> enter deleteFile()" << endl << flush;
    if (file_deleted) {
        minibase_errors.add_error(HEAPFILE, hfErrMsgs[8]);
        return HEAPFILE; // is a delete file still existing?
    }
    Status status;
    HFPage *currDirPage;
    PageId currDirPageId = firstDirPageId, nextDirPageId, currDataPageId;
    RID currDataPageRid, nextDataPageRid;
    DataPageInfo *dpinfop = NULL;
    int recLen;
    
    while (currDirPageId != -1) {
        // cout << "In dirPageId = " << currDirPageId << endl; 
        status = MINIBASE_BM->pinPage(currDirPageId, (Page *&) currDirPage); CHECK_STATUS
        status = currDirPage->firstRecord(currDataPageRid); 
        while (status == OK) {
            status = currDirPage->returnRecord(currDataPageRid, (char *&)dpinfop, recLen); CHECK_STATUS
            assert(recLen == sizeof(DataPageInfo));
            currDataPageId = dpinfop->pageId;
            status = MINIBASE_DB->deallocate_page(currDataPageId); CHECK_STATUS
            // cout << "@ after deallocate_page()" << endl;
            status = currDirPage->nextRecord(currDataPageRid, nextDataPageRid);
            // cout << "next record in dir page. status = " << status << endl;
            currDataPageRid = nextDataPageRid;
        }
        // cout << "out of inner while loop. status = " << status << endl;
        assert(status == DONE);
        nextDirPageId = currDirPage->getNextPage();
        status = MINIBASE_DB->deallocate_page(currDirPageId); CHECK_STATUS
        status = MINIBASE_BM->unpinPage(firstDirPageId); CHECK_STATUS
        currDirPageId = nextDirPageId;
    }
    
    file_deleted = 1;
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
                    RID &rpDataPageRid)
{
    rpDataPageId = rid.pageNo;
    rpDirPageId = firstDirPageId;
    PageId nextDirPageId;
    DataPageInfo *dpinfop = (DataPageInfo*) malloc(sizeof(DataPageInfo));
    RID currDataPageRid, nextDataPageRid;
    Status status;
    char* dummy_ptr = NULL;
    int dummy_recLen;
    int dpinfo_recLen;
    while (rpDirPageId != -1) {
        status = MINIBASE_BM->pinPage(rpDirPageId, (Page*&)rpdirpage); CHECK_STATUS
        status = rpdirpage->firstRecord(currDataPageRid);
        while (status == OK) {
            status = rpdirpage->getRecord(currDataPageRid, (char*)dpinfop, dpinfo_recLen); CHECK_STATUS
            assert(dpinfo_recLen == sizeof(DataPageInfo));
            if (dpinfop->pageId == rpDataPageId) {
                status = MINIBASE_BM->pinPage(rpDataPageId, (Page*&)rpdatapage); CHECK_STATUS
                status = rpdatapage->returnRecord(rid, dummy_ptr, dummy_recLen);
                if (status != OK) {
                    status = MINIBASE_BM->unpinPage(rpDataPageId); CHECK_STATUS
                    status = MINIBASE_BM->unpinPage(rpDirPageId); CHECK_STATUS
                    rpdirpage = NULL; rpdatapage = NULL;
                    free(dpinfop); 
                    return status;
                } else {
                    rpDataPageRid = currDataPageRid;
                    free(dpinfop); 
                    return OK;
                }
            } else {
                status = rpdirpage->nextRecord(currDataPageRid, nextDataPageRid);
                currDataPageRid = nextDataPageRid;
            }
        }
        assert(status == DONE);
        nextDirPageId = rpdirpage->getNextPage();
        status = MINIBASE_BM->unpinPage(rpDirPageId); CHECK_STATUS
        rpDirPageId = nextDirPageId;
    }
    rpdirpage = NULL; rpdatapage = NULL;
    free(dpinfop);
    return FAIL;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // cout << "> enter allocateDirSpace()" << ' ';
    Status status;
    HFPage *dummyDirPage;
    HFPage *allocDirPage = (HFPage *) malloc(sizeof(HFPage));
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
    allocDirPage->setNextPage(-1);
    // cout << "> prev dirPage = " << allocDirPage->getPrevPage() << ' ';
    // cout << "> new dirPage = " << allocDirPageId << ' ';
    // cout << "> next dirPage = " << allocDirPage->getNextPage() << endl;
    // if (allocDirPageId == 66) {
    //     cout << "currDirPageId = 66, nextDirPageId = " << allocDirPage->getNextPage() << endl;
    // }
    status = allocDirPage->insertRecord((char*)dpinfop, sizeof(DataPageInfo), allocDataPageRid); CHECK_STATUS
    // cout << "size of new dirPage2:" << allocDirPage->available_space() << ' ';
	status = MINIBASE_BM->unpinPage(allocDirPageId, TRUE); CHECK_STATUS
    // end_of_dirPage = allocDirPageId;
    return OK;
}
