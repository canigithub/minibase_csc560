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
    // cout << "enter ctor" << endl << flush;
  numBuffers = numbuf;
  bufPool = (Page*)malloc(numbuf * sizeof(Page));
  
  bufDescr = new BufDescr*[numbuf];
  for ( int i = 0; i < numbuf; ++i) {
      bufDescr[i] = new BufDescr();
  }
  
  htDir = (PageToFrameHashEntry**)malloc(HTSIZE * sizeof(PageToFrameHashEntry*));
  for ( int i = 0; i < HTSIZE; ++i) {
      htDir[i] = NULL;
  }
  
  buildReplacementList();
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
    // cout << "enter dtor" << endl << flush;
  flushAllPages();
	if(bufPool == NULL) {
		fprintf(stderr, "Error: buffer pool already deleted!\n");
	} else {
  	free(bufPool);
	}
	bufPool = NULL;
  for (unsigned int i = 0; i < numBuffers; ++i) {
      delete bufDescr[i];
  }
  delete[] bufDescr;
  free(htDir);
  freeReplacementList();
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

void BufMgr::freeReplacementList() {
    RListNode* curListNode = RLHead;
    RListNode* nextListNode = RLHead;
    while(curListNode) {
        nextListNode = curListNode->next;
        delete curListNode;
        curListNode = nextListNode;
    }
}

void BufMgr::printReplacementList() {
	RListNode* curr = RLHead;
	while(curr) {
		if(curr == curr->next) {
			fprintf(stderr, "Loop in replacment list!\n");
			exit(1);
		}
		curr = curr->next;
	}
	printf("\n");
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
	printf("\n\n");
}

int BufMgr::inReplacementList(PageId pageid) {
    RListNode* curr = RLHead;
    while (curr) {
        if (curr->pageid == pageid)
            return 1;
        curr = curr->next;
    }
    return 0;
}

Status BufMgr::removeFromReplacementList(PageId pageid) {
    
    if (RLHead == RLTail) {
        if (RLHead->pageid == pageid) {
            delete RLHead;
            RLHead = NULL;
            RLTail = NULL;
            return OK;
        } else {
            minibase_errors.add_error(BUFMGR, bufErrMsgs[6]);
	        return BUFMGR;
        }
    }
    
    RListNode* curr = RLHead;
    // RListNode* next = RLHead;
    
    if (curr->pageid == pageid) {
        // now curr is head
        RLHead = curr->next;
				RLHead->prev = NULL;
        delete curr;
        return OK;
    }
    
    // advance one node
    curr = curr->next;
    
    while(curr->next) {
        if (curr->pageid == pageid) {
            curr->prev->next = curr->next;
            curr->next->prev = curr->prev;
            delete curr;
            return OK;
        }
        curr = curr->next;
    }
    
    // now curr is the tail
    if (curr->pageid == pageid) {
        RLTail = curr->prev;
				RLTail->next = NULL;
        delete curr;
        return OK;
    }
    
    // something is wrong, couldn't find it
    minibase_errors.add_error(BUFMGR, bufErrMsgs[6]);
	return BUFMGR;
}


int BufMgr::lookUpFrameid(PageId pageid) {
    int htIndex = hash(pageid);
    PageToFrameHashEntry* curEntry = htDir[htIndex];
    while (curEntry) {
          if (curEntry->pageid == pageid) {
              return curEntry->frameid;
          }
          curEntry = curEntry->next;
      }
      return -1; 
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

	// remove the first entry
	if(curr == htDir[htIndex])
		htDir[htIndex] = curr->next;

	delete(curr);

	return OK;

}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
// Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage=FALSE) {
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
  // cout << "enter pinPage(1) page=" << PageId_in_a_DB << " " << flush;
  Status status;
  
  Page* temp_ptr = (Page*)malloc(sizeof(Page));
  status = MINIBASE_DB->read_page(PageId_in_a_DB, temp_ptr);
  if (status != OK) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[7]);
      return BUFMGR;
  }
  free(temp_ptr);
  
  // cout <<"in pinpage:";
  int frameid = lookUpFrameid(PageId_in_a_DB);
  
  if (frameid != -1) {
      // in hash table
      ++(bufDescr[frameid]->pincount);
      page = &bufPool[frameid];
      
      if (inReplacementList(PageId_in_a_DB)) {
				
          status = removeFromReplacementList(PageId_in_a_DB); 
          CHECK_STATUS
      }
       				
      if (!emptyPage && !bufDescr[frameid]->dirty) {
            status = MINIBASE_DB->read_page(PageId_in_a_DB, page); 
            CHECK_STATUS
        }
        
     // cout << "bufdesc:[" << bufDescr[frameid]->pageid << " ";
  // cout << frameid << " " << bufDescr[frameid]->pincount << " " << bufDescr[frameid]->dirty << " ";
  // cout << bufDescr[frameid]->love << "]";
  // cout << endl << flush;
        return OK;
  }
  
  // if the page is not in bufPool means in RList
  
  if (!RLHead) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[7]);
      return BUFMGR;
  }
    
  frameid = RLHead->frameid;
//   cout << frameid << " ";
  assert(bufDescr[frameid]->pincount == 0);
  status = removeFromReplacementList(RLHead->pageid); CHECK_STATUS
    
    PageId page_num = bufDescr[frameid]->pageid;

	addToPFHash(PageId_in_a_DB, frameid);
  if (bufDescr[frameid]->dirty) { 
      flushPage(bufDescr[frameid]->pageid);
  }
  if(page_num != -1)
		status = removeFromPFHashTable(page_num);
  ++(bufDescr[frameid]->pincount);
  bufDescr[frameid]->pageid = PageId_in_a_DB;
  bufDescr[frameid]->dirty = 0;
  bufDescr[frameid]->love = 0;
  page = &bufPool[frameid];
  if (!emptyPage && !bufDescr[frameid]->dirty) {
      // cout <<"not read." << flush;
        status = MINIBASE_DB->read_page(PageId_in_a_DB, page);
        // cout <<"status=" << status; 
        CHECK_STATUS
  }  
  // cout << "bufdesc:[" << bufDescr[frameid]->pageid << " ";
  // cout << frameid << " " << bufDescr[frameid]->pincount << " " << bufDescr[frameid]->dirty << " ";
  // cout << bufDescr[frameid]->love << "]";
  // cout << endl << flush;
  return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty, int hate){
  // cout << "enter unpinPage(1) page=" << page_num << " hate=" << hate << endl << flush;
	
    // cout <<"print bufdescr..." << endl << flush;
    // for (unsigned int i = 0; i < numBuffers; ++i) {
        // cout <<"[" << bufDescr[i]->pageid << " " << i;
        // cout <<" " << bufDescr[i]->pincount << " " << bufDescr[i]->dirty;
        // cout <<" " << bufDescr[i]->love << "]" << endl << flush;
    // }
    
  int frameid = lookUpFrameid(page_num);
	if(frameid > NUMBUF) {
	}
  if (frameid == -1) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[9]);
      return BUFMGR;
  }
  
// cout <<"page num=" << page_num << " frameid=" << frameid << endl << flush;
	if(dirty) {
		bufDescr[frameid]->dirty = 1;
	} 
  
  if (bufDescr[frameid]->pincount <= 0) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[10]);
      return BUFMGR;
  }
  
  --(bufDescr[frameid]->pincount);
  if (!hate) {
		bufDescr[frameid]->love = 1; 
	}
  
  if (bufDescr[frameid]->pincount == 0) {
			
      RListNode* newNode = new RListNode(frameid, page_num);
      if (RLHead == NULL || RLTail == NULL) {
          assert(RLHead == NULL && RLTail == NULL);
          RLHead = newNode;
          RLTail = newNode;   
      } else {
          if (bufDescr[frameid]->love) {
	          newNode->prev = RLTail;
	          RLTail->next = newNode;
	          RLTail = newNode;
	            // if(!RLHead) { 
	            //     RLHead = RLTail;
	            // }
          } else {
	            // if(!RLHead) {
	            //     RLHead = RLTail;
	            // }
	          newNode->next = RLHead;
	          RLHead->prev = newNode;
	          RLHead = newNode;
        	}
      }
	}
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
    // cout << "enter newPage" << endl << flush;
  Status status;
  
  int n_unpinned = getNumUnpinnedBuffers();
  if (n_unpinned == 0) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[7]);
      return BUFMGR;
  }
  
  status = MINIBASE_DB->allocate_page(firstPageId, howmany);
  CHECK_STATUS
  status = pinPage(firstPageId, firstpage, 1); CHECK_STATUS
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
    // cout << "enter freePage" << endl << flush;
  
  Status status;
  int frameid = lookUpFrameid(globalPageId);
  if (bufDescr[frameid]->pincount > 0) {
      minibase_errors.add_error(BUFMGR, bufErrMsgs[11]);
      return BUFMGR;
  }
  status = MINIBASE_DB->deallocate_page(globalPageId); CHECK_STATUS
  
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
    // cout << "enter flushPage" << endl << flush;
	Status status = OK;
  int frameId = lookUpFrameid(pageid);
	Page *pageptr = &bufPool[frameId];
	status = MINIBASE_DB->write_page(pageid, 	pageptr);
	CHECK_STATUS
  return OK;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
	// cout << "enter flushAllPage" << endl << flush;
	Status status;
	int frameid;
	PageId pageid;
	for(frameid = 0; frameid < NUMBUF; frameid++) {
		pageid = bufDescr[frameid]->pageid;
		if(pageid != -1) {
			status = flushPage(pageid);
			CHECK_STATUS
		}
	}
  return OK;
}


/*** Methods for compatibility with project 1 ***/
//Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage=0, const char *filename=NULL){
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
    return pinPage(PageId_in_a_DB, page, emptyPage);
    /*if (filename == NULL) {
        // cout << "enter pinPage(2)" << flush;
        return pinPage(PageId_in_a_DB, page, emptyPage);
    } else {
        minibase_errors.add_error(BUFMGR, bufErrMsgs[6]);
				cerr "Database does not support multiple files (attempted to write to file " << filename << ")" << endl;
	    return BUFMGR;
    }*/
}


//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
// Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename=NULL){
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename=NULL){
        return unpinPage(globalPageId_in_a_DB, dirty, 0);
 /* 
      if (filename == NULL) {
          // cout << "enter unpinPage(2)" << flush;
        return unpinPage(globalPageId_in_a_DB, dirty, 0);
    } else {
        minibase_errors.add_error(BUFMGR, bufErrMsgs[6]);
				cerr << filename << endl;
	    return BUFMGR;
    }
		*/
}


//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
	int i;
	int count = 0;
	for(i = 0; i < NUMBUF; i++) {
		if(bufDescr[i]->pincount == 0)
			count++;
	}
  return count;
}
