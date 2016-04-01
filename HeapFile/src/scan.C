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

#define CHECK_RETURN_STATUS if (status != OK) {returnStatus = status; return;}
#define CHECK_STATUS if (status != OK) {return status;}
#define ASSERT_STATUS assert(status == OK);

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
 	// reset();
    // cout << "in scan destructor";
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
	Status status;
	if(scanIsDone) {reset(); return DONE;}
	rid = userRid;
	status = dataPage->getRecord(rid, recPtr, recLen); CHECK_STATUS
	status = mvNext(userRid);
	// if(status != OK && status != DONE)
	// 	scanIsDone = 1;
    if (status == DONE) { 
        scanIsDone = 1;
    }
    return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
	setbuf(stdout, NULL);
    _hf = hf;
	Status status;
	status = firstDataPage(); CHECK_STATUS
	status = dataPage->firstRecord(userRid); CHECK_STATUS
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
	status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS
	status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
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
	
	Status status;
	int recLen;
	DataPageInfo *dpinfop = (DataPageInfo*) malloc(sizeof(DataPageInfo));

	/* pin the first directory page, grab the Rid for the first Data Page */
	dirPageId = _hf->firstDirPageId;
	status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage); CHECK_STATUS
	status = dirPage->firstRecord(dataPageRid); CHECK_STATUS

	/* grab the DataPageInfo* describing the first data page, containing the data page ID */
	status = dirPage->getRecord(dataPageRid, (char *) dpinfop, recLen); CHECK_STATUS
	assert(recLen == sizeof(DataPageInfo));
	dataPageId = dpinfop->pageId;

	/* pin the first data page, get its first record */ 
	status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);  CHECK_STATUS
    //cout << "firstDataPageId:" << dataPageId << '\t';
	free(dpinfop);
    return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
	// try to find it in the same directory page
	Status status;
	// status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
	RID nextDataPageRid;
	status = dirPage->nextRecord(dataPageRid, nextDataPageRid);
    //cout << "status@nextDataPage().nextRecord():" << status << '\t';
	dataPageRid = nextDataPageRid;
	int recLen;

	DataPageInfo *dpinfop = (DataPageInfo*) malloc(sizeof(DataPageInfo));
	if(status == DONE) {
		status = nextDirPage(); CHECK_STATUS
        //cout << "status@nextDataPage().nextDirPage():" << status << '\t'; 
		status = dirPage->firstRecord(dataPageRid); CHECK_STATUS
	} 
	
    status = MINIBASE_BM->unpinPage(dataPageId); CHECK_STATUS
    
	status = dirPage->getRecord(dataPageRid, (char *)dpinfop, recLen); CHECK_STATUS
	// if(status == DONE) return DONE;
	dataPageId = dpinfop->pageId;

	status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    free(dpinfop);
    return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
	Status status;
	PageId nextDirPageId = dirPage->getNextPage();
    if (nextDirPageId == -1) return DONE;
    //cout << "nextDirPageId@nextDirPage():" << nextDirPageId << '\t';
	status = MINIBASE_BM->unpinPage(dirPageId); CHECK_STATUS
	dirPageId = nextDirPageId;
	status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage); CHECK_STATUS
    return OK;
}

Status Scan::mvNext(RID& rid) {
	Status status;
	RID nextRid;
	status = dataPage->nextRecord(userRid, nextRid);
    //cout << "curRid:" << userRid.pageNo << ',' << userRid.slotNo << '\t'; 
    //cout << "nextRid:" << nextRid.pageNo << ',' << nextRid.slotNo << '\t';
	if(status == DONE) {
		status = nextDataPage(); CHECK_STATUS
		// if(status == DONE) return DONE;
		status = dataPage->firstRecord(nextRid); ASSERT_STATUS
	}
    //cout << "mvNext_status:" << status << '\t';
	rid = nextRid;
	return status;
}
