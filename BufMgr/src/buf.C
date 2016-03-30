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
		printf("Building replacement list, RLHead had address %ld\n", (unsigned long) RLHead);
    RListNode* curListNode = RLHead;
    for(unsigned int i = 1; i < numBuffers; ++i) {
        curListNode->next = new RListNode(i);
        curListNode->next->prev = curListNode;
        curListNode = curListNode->next;
    }
    RLTail = curListNode;
		printf("Exiting buildReplacementList, RLHead has frameid%d\n", RLHead->frameid);
		printf("** changed replacement list **\n");
		printReplacementList();
}

void BufMgr::printReplacementList() {
	RListNode* curr = RLHead;
	while(curr) {
		printf("[%d/]->", curr->frameid);
		curr = curr->next;
	}
	printf("\n\n");
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

void BufMgr::printLinkedList(int htIndex) {
	PageToFrameHashEntry* curr = htDir[htIndex];
	if(!curr) {
		printf("%d points to an empty bucket\n", htIndex);
		return;
	}
	while(curr) {
		printf("[%d, %d]->", curr->pageid, curr->frameid);
		curr = curr->next;
	}
	printf("\n");
}

void BufMgr::addToPFHash(PageId pageid, int frameid) {
	PageToFrameHashEntry* newEntry = new PageToFrameHashEntry(pageid, frameid);

  int htIndex = hash(pageid);
	//printf("About to add to hash bucket %d\n", htIndex);
	//printLinkedList(htIndex);
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

	printf("After add to hash bucket %d:", htIndex);
	printLinkedList(htIndex);
	printf("\n");

}

// called from unpinPage

Status BufMgr::removeFromPFHashTable(PageId pageid) {
	int htIndex = hash(pageid);
	PageToFrameHashEntry* curr = htDir[htIndex]; 

	//printf("About to remove page %d from hash bucket %d\n", pageid, htIndex);
	//printLinkedList(htIndex);
  
	// bucket is empty
	if(!curr) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[2]);
			return BUFMGR;
	}

	// advance to correct entry, stop if we reach the end
	while(curr->next && curr->pageid != pageid)
		curr = curr->next;

	//printf("Stopped traversing at entry with pageid %d\n", curr->pageid);

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

	// remove the first entry
	if(curr == htDir[htIndex])
		htDir[htIndex] = curr->next;

	delete(curr);

	printf("After remove from hash bucket %d", htIndex);
	printLinkedList(htIndex);
	printf("\n");

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
	// printf("Didn't find page %d in hash table\n", PageId_in_a_DB);
  
  // if the page is not in bufPool means in RList
  RListNode* curRListNode = RLHead;
  if (!curRListNode) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[5]);
      return BUFMGR;
  }

	printf("Using buffer pool frame %d for page %d\n", curRListNode->frameid, PageId_in_a_DB);
  
	if(!RLHead->next)
		printf("buf.C line 209: At end of replacment list!\n");
//	printf("Advancing RLHead from %ld ", (unsigned long) RLHead);
  RLHead = RLHead->next;
//	printf("to %ld\n", (unsigned long) RLHead);
//	printf("Replacement list:");
	printReplacementList();
  
  frameid = curRListNode->frameid;
  delete curRListNode;

	PageId page_num = bufDescr[frameid]->pageid;

	addToPFHash(PageId_in_a_DB, frameid);
  
  if (bufDescr[frameid]->dirty) {
			printf("content of frame %d is dirty, writing to disk\n", frameid);
			printf("Content written: %s\n", (char*)&bufPool[frameid]); 
      flushPage(bufDescr[frameid]->pageid);
  }
  
  assert(bufDescr[frameid]->pincount == 0);

	if(page_num != -1)
		status = removeFromPFHashTable(page_num);
  
  ++(bufDescr[frameid]->pincount);
  bufDescr[frameid]->pageid = PageId_in_a_DB;
  bufDescr[frameid]->dirty = 0;
  bufDescr[frameid]->love = 0;
	//printf("address of bufPool is %ld\n", bufPool);
	// printf("Assiging address %ld to page\n", &bufPool[frameid]);
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
  
	printf("\n\nUsing unpinPage version 1\n");
  int frameid = lookUpFrameid(page_num);
  if (frameid == -1) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[9]);
      return BUFMGR;
  }

	if(dirty) {
		printf("Unpinning page %d stored at frameid %d as dirty\n", page_num, frameid);
		bufDescr[frameid]->dirty = 1;
	} else {
		printf("Page %d at frameid %d is _not_ dirty\n", page_num, frameid);
	}
  
  if (bufDescr[frameid]->pincount <= 0) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[10]);
      return BUFMGR;
  }
  
  --(bufDescr[frameid]->pincount);
  if (!hate) {
		printf("* Page %d is designated as loved\n", page_num);
		bufDescr[frameid]->love = 1; 
	}
  
  if (bufDescr[frameid]->pincount == 0) {
			printf("Pin count is 0; adding frame %d to replacement list, recalling that page %d is ", frameid, page_num);
      RListNode* newNode = new RListNode(frameid);
      if (bufDescr[frameid]->love) {
					printf("loved\n");
          newNode->prev = RLTail;
          RLTail->next = newNode;
          RLTail = newNode;

					if(!RLHead) {
						RLHead = RLTail;
					}
      } else {
					printf("hated\n");
					if(!RLHead) {
						RLHead = RLTail;
					}
          newNode->next = RLHead;
          RLHead->prev = newNode;
          RLHead = newNode;
      }
			printReplacementList();
  }

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
	Status status = OK;
  int frameId = lookUpFrameid(pageid);
	printf("got frameId %d from hash table\n", frameId);
	Page *pageptr = &bufPool[frameId];
	printf("In flushPage\n");
	printf("Writing to page %d:\n", pageid);
	printf("%s\n", (char*)pageptr);
	printf("Now calling write_page\n");
	status = MINIBASE_DB->write_page(pageid, 	pageptr);
	CHECK_STATUS
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
	printf("Didn't find page %d in hash table\n", PageId_in_a_DB);
  
  // if the page is not in bufPool means in RList
  RListNode* curRListNode = RLHead;
  if (!curRListNode) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[5]);
      return BUFMGR;
  }

	printf("Using buffer pool frame %d for page %d\n", curRListNode->frameid, PageId_in_a_DB);
  
	printf("Incrementing RLHead from %ld ", (unsigned long) RLHead);
  RLHead = RLHead->next;
	printf("to %ld\n", (unsigned long) RLHead);
	printf("** changed replacement list **\n");
	printReplacementList();
  
  frameid = curRListNode->frameid;
	printf("Deleting old head, at address %ld\n", (unsigned long) curRListNode);
  delete curRListNode;

	PageId page_num = bufDescr[frameid]->pageid;
	if(page_num != -1)
		status = removeFromPFHashTable(page_num);

	addToPFHash(PageId_in_a_DB, frameid);
  
  if (bufDescr[frameid]->dirty) {
			printf("content of frame %d is dirty, writing to disk\n", frameid);
			printf("Content written: %s\n", (char*)&bufPool[frameid]); 
      flushPage(bufDescr[frameid]->pageid);
  }
  
  assert(bufDescr[frameid]->pincount == 0);
  
  ++(bufDescr[frameid]->pincount);
  bufDescr[frameid]->pageid = PageId_in_a_DB;
  bufDescr[frameid]->dirty = 0;
  bufDescr[frameid]->love = 0;
	//printf("address of bufPool is %ld\n", bufPool);
	// printf("Assiging address %ld to page\n", &bufPool[frameid]);
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
  
	printf("Using unpinpage version 2\n");
  int frameid = lookUpFrameid(globalPageId_in_a_DB);
  if (frameid == -1) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[9]);
      return BUFMGR;
  }

	if(dirty) {
		printf("page %d stored at frameid %d is dirty\n", globalPageId_in_a_DB, frameid);
		bufDescr[frameid]->dirty = 1;
	}
  
  if (bufDescr[frameid]->pincount <= 0) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[10]);
      return BUFMGR;
  }
  
  --(bufDescr[frameid]->pincount);
  
  if (bufDescr[frameid]->pincount == 0) {
			printf("Reassigning RLHead from %ld ", (unsigned long) RLHead);
      RListNode* newNode = new RListNode(frameid);
      newNode->next = RLHead;
      RLHead->prev = newNode;
      RLHead = newNode;
			printf("to %ld\n", (unsigned long) RLHead);
			printf("** changed replacement list **\n");
			printReplacementList();
  }

  return OK;

}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  //put your code here
  return 0;
}
