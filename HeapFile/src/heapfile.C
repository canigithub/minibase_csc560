#include "heapfile.h"

#define CHECK_STATUS if(status != OK) { returnStatus = status; return; }


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

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )

	// We assume the file already existed
	Status status = get_file_entry(name, &firstDirPageId);
	// If not, ask the database to allocate a new page use it for a new file entry, and make it a directory page
	if(status != OK)  {								
		HFPage *firstDirPage;
		status = MINIBASE_DB.allocate_page(firstDirPageId, 1); 		CHECK_STATUS ;
		status = MINIBASE_DB.add_file_entry(name, firstDirPageId); 	CHECK_STATUS ;
		status = DATABASE_BM.pinPage(firstDirPageId, firstDirPage); CHECK_STATUS ;
		firstDirPage->init(firstDirPageId);
		status = DATABASE_BM.unpinPage(firstDirPageId);							CHECK_STATUS ;
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
	if(strcmp(filename, ""))
		return;

	HFPage *currDirPage = NULL;
	status = DATABASE_BM.pinPage(firstDirPageId, currDirPage); CHECK_STATUS ;
	PageId currDirPageId = firstDirPageId;
	PageId nextDirPageId = firstDirPage->getNextPage();
	PageId currDataPageId, nextDataPageId;
	RID curRid, nextRid;
	DataPageInfo *dpinfop = NULL;
	int recLen;

	while(currDirPageId != -1) {
		if(!currDirPage->empty()) {
			status = currDirPage->firstRecord(&curRid);
			if(status == OK) {
				status = currDirPage->getRecord(curRid, (char *) dpinfop, &recLen);
				assert(recLen == sizeof(DataPageInfo));
				curDataPageId = dpinfop->pageId;
			}
		}
		currDataPage = currDirPage->getRecord
		nextDirPageId = currDirPage->getNextPage();
		deallocate_page(currDirPageId);
		currDirPageId = nextDirPageId();
		}
		// iterate through directory pages, deallocating each data page
			// look at
	 	// deallocate all the pages?
	 	MINIBASE_DB.deallocate_page(firstDirPageId); // last thing we do
	}

}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
	int num_records = 0;
	PageId currDir = firstDirPageId;
	RID currPageRecord;
	HFPage dirPage;
	DataPageInfo *dpinfop;
	int recLen
	while(!currDir == -1) {
		status = MINIBASE_BM.pinPage(currDir, &dirPage);  CHECK_STATUS ;
		status = dirPage->firstRecord(currPageRecord);	
		while(status == OK) {
			dirPage->getRecord(currPageRecord, (char *) dpinfop, recLen);
			assert(recLen == sizeof(DataPageInfo));
			num_records += dpinfo->recct;
			status = dirPage->nextRecord(currPageRecord);
		}
		if(status != DONE)
			return status;
		currDir = dirPage->getNextPage();
	}

  return num_records;;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
		// assumption: Directory page is a HFPage
		PageId curr = firstDirPageId;
    // fill in the body
    return OK;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
  // fill in the body
  return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
  // fill in the body
  return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
	PageId pageNo = rid->pageNo;
	int slotNo = rid->slotNo;
	PageId curr = firstDirPageId;
  // fill in the body 
  return OK;
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
    // fill in the body
    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
		PageId pageId;
		HFPage *newPage;
		Status status = newpage(&pageId, &newPage, 1);
		if(status != OK)
			return status;
		dpinfo->pageId = pageId;
		dpinfo->recct = 0;
		dpinfo->availspace = newPage->available_space();
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
    // fill in the body
    return OK;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // fill in the body
    return OK;
}

// *******************************************
