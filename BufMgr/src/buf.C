/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/


#include "buf.h"
#include <assert.h>
#include <csignal>

#define CHECK_STATUS if (status != OK) {return status;}

// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",
  "Inserting a duplicate entry in the hash table",
  "Removing a non-existing entry from the hash table",
  "Page not in hash table",
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
  // put your code here
  numBuffers = numbuf;
  bufPool = (Page*)malloc(numbuf * sizeof(Page));
  
  bufDescr = new BufDescr*[numbuf];
  for (int i = 0; i < numbuf; ++i) {
      bufDescr[i] = new BufDescr();
  }
  
  htDir = (PageToFrameHashEntry**)malloc(HTSIZE * sizeof(PageToFrameHashEntry*));
  for (int i = 0; i < HTSIZE; ++i) {
      htDir[i] = NULL;
  }
  
  buildReplacementList();
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
  // put your code here
}

void BufMgr::buildReplacementList() {
    RLHead = new RListNode(0);
    RListNode* curListNode = RLHead;
    for(unsigned int i = 1; i < numBuffers; ++i) {
        curListNode->next = new RListNode(i);
        curListNode->next->prev = curListNode;
        curListNode = curListNode->next;
    }
    RLTail = curListNode;
}

int BufMgr::lookUpFrameid(PageId pageid) {
		//std::raise(SIGINT);
    int htIndex = hash(pageid);
    PageToFrameHashEntry* curEntry = htDir[htIndex];
    while (curEntry) {
          if (curEntry->pageid == pageid) {
              return curEntry->frameid;
          }
          curEntry = curEntry->next;
      }
      return -1; // return invalid frameid
}

void BufMgr::addToPFHash(PageId pageid, int frameid) {
	PageToFrameHashEntry* newEntry = new PageToFrameHashEntry(pageid, frameid);

  int htIndex = hash(pageid);
	PageToFrameHashEntry* curr = htDir[htIndex];

	if((htDir[htIndex]) == NULL) {
		htDir[htIndex] = newEntry;
	} else {
		while(curr->next) {
			curr = curr->next;
		}
		curr->next = newEntry;
		newEntry->prev = curr;
	}


}

// called from unpinPage

Status BufMgr::removeFromPFHashTable(PageId pageid) {
	int htIndex = hash(pageid);
	PageToFrameHashEntry* curr = htDir[htIndex]; 

	// bucket is empty
	if(!curr) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[2]);
			return BUFMGR;
	}

	// advance to correct entry, stop if we reach the end
	while(curr->next && curr->pageid != pageid)
		curr = curr->next;

	// page was not in the hash table
	if(curr->pageid != pageid) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[4]);	
			return BUFMGR;
	}

	// if curr is not the first entry in the table...
	if(curr->prev)
		curr->prev->next = curr->next;

	// if curr is not the last entry in the table...
	if(curr->next)
		curr->next->prev = curr->prev;

	// curr is the only entry in the hash table
	if(!curr->next && !curr->prev)
		htDir[htIndex] = NULL;

	delete(curr);
	return OK;

}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
// Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage=FALSE) {
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
  
  Status status;
  
  int frameid = lookUpFrameid(PageId_in_a_DB);
  if (frameid != -1) {
      ++(bufDescr[frameid]->pincount);
      page = &bufPool[frameid]; 				// ??
      if (!emptyPage) {
            status = MINIBASE_DB->read_page(PageId_in_a_DB, page); 
            CHECK_STATUS
        } 
        return OK;
  }
  
  // if the page is not in bufPool means in RList
  RListNode* curRListNode = RLHead;
  if (!curRListNode) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[5]);
      return BUFMGR;
  }
  
  RLHead = RLHead->next;
  
  frameid = curRListNode->frameid;
  delete curRListNode;


	addToPFHash(PageId_in_a_DB, frameid);
  
  if (bufDescr[frameid]->dirty) {
      flushPage(bufDescr[frameid]->pageid);
  }
  
  assert(bufDescr[frameid]->pincount == 0);
  
  ++(bufDescr[frameid]->pincount);
  bufDescr[frameid]->pageid = PageId_in_a_DB;
  bufDescr[frameid]->dirty = 0;
  bufDescr[frameid]->love = 0;
  page = &bufPool[frameid];
  if (!emptyPage) {
        status = MINIBASE_DB->read_page(PageId_in_a_DB, page); 
        CHECK_STATUS
  }  
  return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
  
  Status status;
  int frameid = lookUpFrameid(page_num);
  if (frameid == -1) {
			printf("1\n");
      minibase_errors.add_error(BUFMGR, bufErrMsgs[9]);
      return BUFMGR;
  }
  
  if (bufDescr[frameid]->pincount <= 0) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[10]);
      return BUFMGR;
  }
  
  --(bufDescr[frameid]->pincount);
  if (!hate) bufDescr[frameid]->love = 1; 
  
  if (bufDescr[frameid]->pincount == 0) {
      RListNode* newNode = new RListNode(frameid);
      if (bufDescr[frameid]->love) {
          newNode->prev = RLTail;
          RLTail->next = newNode;
          RLTail = newNode;
      } else {
          newNode->next = RLHead;
          RLHead->prev = newNode;
          RLHead = newNode;
      }
  }

	status = removeFromPFHashTable(page_num);
	CHECK_STATUS
  
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
  // put your code here
  return OK;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
  //put your code here
  return OK;
}


/*** Methods for compatibility with project 1 ***/
// Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage=0, const char *filename=NULL){
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){

  Status status;
  
  int frameid = lookUpFrameid(PageId_in_a_DB);
  if (frameid != -1) {
      ++(bufDescr[frameid]->pincount);
      page = &bufPool[frameid]; 				// ??
      if (!emptyPage) {
            status = MINIBASE_DB->read_page(PageId_in_a_DB, page); 
            CHECK_STATUS
        } 
        return OK;
  }
  
  // if the page is not in bufPool means in RList
  RListNode* curRListNode = RLHead;
  if (!curRListNode) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[5]);
      return BUFMGR;
  }
  
  RLHead = RLHead->next;
  
  frameid = curRListNode->frameid;
  delete curRListNode;
	addToPFHash(PageId_in_a_DB, frameid);
  
  if (bufDescr[frameid]->dirty) {
      flushPage(bufDescr[frameid]->pageid);
  }
  
  assert(bufDescr[frameid]->pincount == 0);
  
  ++(bufDescr[frameid]->pincount);
  bufDescr[frameid]->pageid = PageId_in_a_DB;
  bufDescr[frameid]->dirty = 0;
  bufDescr[frameid]->love = 0;
  page = &bufPool[frameid];
  if (!emptyPage) {
        status = MINIBASE_DB->read_page(PageId_in_a_DB, page); 
        CHECK_STATUS
  }  
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
// Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename=NULL){
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename=NULL){
  

  Status status;
  int frameid = lookUpFrameid(globalPageId_in_a_DB);
  if (frameid == -1) {
			printf("2\n");
      minibase_errors.add_error(BUFMGR, bufErrMsgs[9]);
      return BUFMGR;
  }
  
  if (bufDescr[frameid]->pincount <= 0) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[10]);
      return BUFMGR;
  }
  
  --(bufDescr[frameid]->pincount);
  
  if (bufDescr[frameid]->pincount == 0) {
      RListNode* newNode = new RListNode(frameid);
      newNode->next = RLHead;
      RLHead->prev = newNode;
      RLHead = newNode;
  }

	status = removeFromPFHashTable(globalPageId_in_a_DB);
	CHECK_STATUS
  
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  //put your code here
  return 0;
}
