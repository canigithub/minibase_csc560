/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan (HeapFile *hf, Status& status)
{
    status = init(hf);
//    status = OK;
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
	dirPageId = _hf->firstDirPageId;
	Status status;
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
  // put your code here
  return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
  _hf = hf;

	Status status;
	status = firstDataPage(); CHECK_STATUS ;
	status = dataPage->firstRecord(userRid); CHECK_STATUS ;
	nxtUserStatus = 1;		// we haven't actually examined this record yet

	scanIsDone = 0;		// arbitrarily decide to check scan is done

	return status;
}

// *******************************************
// Reset everything and unpin all pages.
// We should only have to unpin the current directory page and data page
// When would this actually be used?
Status Scan::reset()
{
	Status status;
	status = MINIBASE_BM.unpinPage(dirPageId); CHECK_STATUS ;
	status = MINIBASE_BM.unpinPage(dataPageId);
  return OK;
}

// *******************************************
// Copy data about first page in the file.
// This should only be called once, during init()
// Side effects: 
//   * pins the first directory page
//   * pins the first data page
//   * sets private data members: dirPageId, dirPage, dataPageRid, dataPageId, dataPage, userRid,  nxtUserStatus
Status Scan::firstDataPage()
{
	
	int recLen;
	DataPageInfo *dpinfop = NULL;

	// pin the first directory page, grab the Rid for the first Data Page
	dirPageId = _hf->firstDirPageId;
	status = MINIBASE_BM.pinPage(dirPageId, (Page *) dirPage); CHECK_STATUS;
	status = dirPage->firstRecord(dataPageRid);  	CHECK_STATUS;

	// grab the DataPageInfo* describing the first data page, containing the data page ID
	status = dirPage.getRecord(dataPageRid, (char *) dpinfop, recLen); CHECK_STATUS ;
	assert(recLen == sizeof(DataPageInfo));
	dataPageId = dpinfop->pageId;

	// pin the first data page, get its first record 
	status = MINIBASE_BM.pinPage(dataPageId, (Page *) dataPage);  CHECK_STATUS ;

  return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
	// try to find it in the same directory page
	status = MINIBASE_BM.unpinPage(dataPageId);
	status = dirPage->nextRecord(dataPageId);
	if(status == DONE) {
		status = nextDirPage(); CHECK_STATUS;
		status = dirPage->firstRecord(dataPageId); CHECK_STATUS ;
	}
	status = MINIBASE_BM.pinPage(dataPageId, (Page *) dataPage);
  return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
	PageId nextDirPageId = dirPage->getNextPage();
	status = MINIBASE_BM.unpinPage(dirPageId); 		CHECK_STATUS ;
	dirPageId = nextDirPageId;
	status = MINIBASE_BM.pinPage(dirPageId, dirPage); CHECK_STATUS ;
  return OK;
}
