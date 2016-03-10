//*****************************************
//  Driver program for heapfiles
//****************************************

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "db.h"
#include "heapfile.h"
#include "scan.h"
#include "heap_driver.h"
#include "buf.h"

using namespace std;

static const int namelen = 24;
struct Rec
{
    int ival;
    float fval;
    char name[namelen];
};

static const int reclen = sizeof(Rec);



HeapDriver::HeapDriver() : TestDriver( "hftest" )
{
   choice = 2000;   // big enough for file to occupy > 1 directory page
  
}

HeapDriver::~HeapDriver()
{}


Status HeapDriver::runTests()
{
    return TestDriver::runTests();
}

Status HeapDriver::runAllTests()
{
    Status answer;
    minibase_globals = new SystemDefs(answer,dbpath,logpath,
                                      1000,500,100,"Clock");
    if ( answer == OK )
        answer = TestDriver::runAllTests();


    delete minibase_globals;
    return answer;
}

const char* HeapDriver::testName()
{
    return "Heap File";
}


//********************************************

int HeapDriver::test1()
{
    cout << "\n  Test 1: Insert and scan fixed-size records\n";
    Status status = OK;
    RID rid;

    cout << "  - Create a heap file\n";
    HeapFile f("file_1", status);

    if (status != OK)
        cerr << "*** Could not create heap file\n";
    else if ( MINIBASE_BM->getNumUnpinnedBuffers()
                != MINIBASE_BM->getNumBuffers() ) {
        cerr << "*** The heap file has left pages pinned\n";
        status = FAIL;
    }
    
    if ( status == OK )
      {
        cout << "  - Add " << choice << " records to the file\n";
        for (int i =0; (i < choice) && (status == OK); i++)
          {
            Rec rec = { i, i*2.5 };
            sprintf(rec.name, "record %i",i);
            
            // cout << endl << ">>>i:" << i << " ";
            status = f.insertRecord((char *)&rec, reclen, rid);
        
			//			cout << rid.slotNo << " ";

            if (status != OK) {
                cerr << "*** Error inserting record " << i << endl;
				printf("Status %d\n", status);
            } else if ( MINIBASE_BM->getNumUnpinnedBuffers()
                            != MINIBASE_BM->getNumBuffers() ) {
                cerr << "*** Insertion left a page pinned\n";
                status = FAIL;
            }
          }

        if ( f.getRecCnt() != choice )  
          {
            status = FAIL;
            cerr << "*** File reports " << f.getRecCnt() << " records, not "
                 << choice << endl;
          }
      }

//       cout << "after insertion 2000 finished" << endl;
      
//     PageId firstDirPageId = f.getFirstDirPageId(), nextId;
//     cout << ">file_1: ";
//     HFPage *firstDirPage;
//     while (firstDirPageId != -1) {
//         cout << firstDirPageId << ", ";
//         status = MINIBASE_BM->pinPage(firstDirPageId, (Page *&)firstDirPage); if (status != OK) return status;
//         nextId = firstDirPage->getNextPage();
//         status = MINIBASE_BM->unpinPage(firstDirPageId); if (status != OK) return status;    
//         firstDirPageId = nextId;
//     }
//     cout << endl;
    
//    cout << "finish test1 tests" << endl;


      // In general, a sequential scan won't be in the same order as the
      // insertions.  However, we're inserting fixed-length records here, and
      // in this case the scan must return the insertion order.

    Scan* scan = 0;
    if ( status == OK )
      {
        cout << "  - Scan the records just inserted\n";
        scan = f.openScan(status);
				// printf("opened the scan\n");

        if (status != OK)
            cerr << "*** Error opening scan\n";
        else if ( MINIBASE_BM->getNumUnpinnedBuffers() == MINIBASE_BM->getNumBuffers() )
          {
            cerr << "*** The  heap-file scan has not pinned the first page\n";
			cerr << MINIBASE_BM->getNumUnpinnedBuffers() - MINIBASE_BM->getNumBuffers() << endl;
            status = FAIL;
          }
      }

    if ( status == OK )
      {
        int len, i = 0;
        Rec rec;
        // cout << "ready to sacn:" << endl;
        while ( (status = scan->getNext(rid, (char *)&rec, len)) == OK )
          {
            //   cout << endl << "(" << rid.pageNo << "," << rid.slotNo <<")";  
			if ( len != reclen )
              {
                cerr << "*** Record " << i << " had unexpected length " << len
                     << endl;
                status = FAIL;
                break;
              }
            else if ( MINIBASE_BM->getNumUnpinnedBuffers()
                                    == MINIBASE_BM->getNumBuffers() )
              {
                cerr << "On record " << i << ":\n";
                cerr << "*** The heap-file scan has not left its page pinned\n";
                status = FAIL;
                break;
              }

            char name[ sizeof rec.name ];
            sprintf( name, "record %i", i );
            
            // cout << rec.ival << '\t' << rec.fval << '\t' << rec.name << endl;
            
            if( (rec.ival != i) 
                || (rec.fval != i*2.5)
                || (0 != strcmp( rec.name, name )) )
              {
                cerr << "*** Record " << i
                     << " differs from what we inserted\n";
                cerr << "rec.ival: "<< rec.ival
                     << " should be " << i << endl;
                cerr << "rec.fval: "<< rec.fval
                     << " should be " << (i*2.5) << endl;
                cerr << "rec.name: " << rec.name
                     << " should be " << name << endl;
                status = FAIL;
                break;
              }
            ++i;
          }
          
        // cout << "finish scan while loop." << endl;
        // cout << "i = " << i << endl;
        if ( status == DONE )
          {
            if ( MINIBASE_BM->getNumUnpinnedBuffers() != MINIBASE_BM->getNumBuffers() ) {
		  		cerr << MINIBASE_BM->getNumUnpinnedBuffers() << endl;
				cerr << MINIBASE_BM->getNumBuffers() << endl;
                cerr << "*** The heap-file scan has not unpinned its page after finishing\n"; }
            else if ( i == choice )
                status = OK;
            else
                cerr << "*** Scanned " << i << " records instead of "
                     << choice << endl;
          }
      }

    delete scan;

    if ( status == OK )
        cout << "  Test 1 completed successfully.\n"; 
    
    return (status == OK);
    // return OK;
}


//***************************************************

int HeapDriver::test2()
{
    cout << "\n  Test 2: Delete fixed-size records\n";
    Status status = OK;
    Scan* scan = 0;
    RID rid;
    
    // cout << "before open the file." << endl;
    // return (status = OK);
    
    cout << "  - Open the same heap file as test 1\n";
    HeapFile f("file_1", status);

    // cout << ">>> after opened the file. status = " << status << endl;
    // return (status = OK);
    
    if (status != OK)
        cerr << "*** Error opening heap file\n";
        

    if ( status == OK )
      {
        cout << "  - Delete half the records\n";
        scan = f.openScan(status);
        if (status != OK)
            cerr << "*** Error opening scan\n";
      }
    if ( status == OK )
      {
        int len, i = 0;
        Rec rec;

        while ( (status = scan->getNext(rid, (char *)&rec, len)) == OK )
          {
            //cout << "i is " << i << endl;
            //cout << "rid is " << rid << endl;
            if ( i & 1 )        // Delete the odd-numbered ones.
              {
                // cout << "To delete record " << rid << endl;
                // cout << ">>>i:" << i << '\t';
                // cout << "delete #" << i << endl;
                status = f.deleteRecord( rid );
				// cout << " To delete record " << rid.slotNo << endl;
                if ( status != OK )
                  {

                    cerr << "*** Error deleting record " << i << endl;
					minibase_errors.show_errors();
                    break;
                  }

              }
            ++i;
          }

        if ( status == DONE )
            status = OK;
      }

    delete scan;
    scan = 0;
    
    // return (status = OK);
    
    if ( status == OK
         && MINIBASE_BM->getNumUnpinnedBuffers() != MINIBASE_BM->getNumBuffers() )
      {
        cerr << "*** Deletion left a page pinned\n";
        status = FAIL;
      }

    if ( status == OK )
      {
        cout << "  - Scan the remaining records\n";
        scan = f.openScan(status);
        if (status != OK)
            cerr << "*** Error opening scan\n";
      }
    if ( status == OK )
      {
        int len, i = 0;
        Rec rec;

        while ( (status = scan->getNext(rid, (char *)&rec, len)) == OK )
          {
            // cout << "retrieve #" << i << endl;
            if( (rec.ival != i)  ||
                (rec.fval != i*2.5) )
              {
                cerr << "*** Record " << i
                     << " differs from what we inserted\n";
                cerr << "rec.ival: "<< rec.ival
                     << " should be " << i << endl;
                cerr << "rec.fval: "<< rec.fval
                     << " should be " << (i*2.5) << endl;
                status = FAIL;
                break;
              }
            i += 2;     // Because we deleted the odd ones...
          }

        if ( status == DONE )
            status = OK;
      }

    delete scan;

    if ( status == OK )
        cout << "  Test 2 completed successfully.\n";
        
    // cout << "# unpined buf = " << MINIBASE_BM->getNumUnpinnedBuffers() << endl; 
    // cout << "# bufs = " << MINIBASE_BM->getNumBuffers() << endl;
    
    return (status == OK);
    // return OK;
}


//********************************************************

int HeapDriver::test3()
{
    cout << "\n  Test 3: Update fixed-size records\n";
    Status status = OK;
    Scan* scan = 0;
    RID rid;

    cout << "  - Open the same heap file as tests 1 and 2\n";
    HeapFile f("file_1", status);

    if (status != OK)
        cerr << "*** Error opening heap file\n";


    if ( status == OK )
      {
        cout << "  - Change the records\n";
        scan = f.openScan(status);
        if (status != OK)
            cerr << "*** Error opening scan\n";
      }
    if ( status == OK )
      {
        int len, i = 0;
        Rec rec;

        while ( (status = scan->getNext(rid, (char *)&rec, len)) == OK )
          {
            rec.fval = 7*i;     // We'll check that i==rec.ival below.
            status = f.updateRecord( rid, (char*)&rec, len );
            // cout << ">>>i:" << i << endl;
            if ( status != OK )
              {
                cerr << "*** Error updating record " << i << endl;
                break;
              }
            i += 2;     // Recall, we deleted every other record above.
          }

        if ( status == DONE )
            status = OK;
      }
      
    // cout << "before delete scan." << endl;
    delete scan;
    scan = 0;

    if ( status == OK
         && MINIBASE_BM->getNumUnpinnedBuffers() != MINIBASE_BM->getNumBuffers() )
      {
        cerr << "*** Updating left pages pinned\n";
        status = FAIL;
      }

    if ( status == OK )
      {
        cout << "  - Check that the updates are really there\n";
        scan = f.openScan(status);
        if (status != OK)
            cerr << "*** Error opening scan\n";
      }
    if ( status == OK )
      {
        int len, i = 0;
        Rec rec, rec2;

        while ( (status = scan->getNext(rid, (char *)&rec, len)) == OK )
          {
              // While we're at it, test the getRecord method too.
            status = f.getRecord( rid, (char*)&rec2, len );
            if ( status != OK )
              {
                cerr << "*** Error getting record " << i << endl;
                break;
              }
            if( (rec.ival != i) || (rec.fval != i*7)
                || (rec2.ival != i) || (rec2.fval != i*7) )
              {
                cerr << "*** Record " << i
                     << " differs from our update\n";
                cerr << "rec.ival: "<< rec.ival
                     << " should be " << i << endl;
                cerr << "rec.fval: "<< rec.fval
                     << " should be " << (i*7.0) << endl;
                status = FAIL;
                break;
              }
            i += 2;     // Because we deleted the odd ones...
          }

        // cout << "exit 2nd while loop in test3" << endl;
        
        if ( status == DONE )
            status = OK;
      }
      
      
      
    delete scan;
  
    if ( status == OK )
        cout << "  Test 3 completed successfully.\n";
    
    // cout << "# unpined buf = " << MINIBASE_BM->getNumUnpinnedBuffers() << endl; 
    // cout << "# bufs = " << MINIBASE_BM->getNumBuffers() << endl;
    
    // PageId firstDirPageId = f.getFirstDirPageId();
    // cout << ">file_1: ";
    // HFPage *firstDirPage;
    // status = MINIBASE_BM->pinPage(firstDirPageId, (Page *&)firstDirPage); if (status != OK) return status;
    // while (firstDirPageId != -1) {
    //     firstDirPageId = firstDirPage->getNextPage();
    //     cout << firstDirPageId << ", ";
    // }
    // cout << endl;
    // status = MINIBASE_BM->unpinPage(firstDirPageId); if (status != OK) return status;
    
    return (status == OK);
    // return OK;
}


//*****************************************************************

int HeapDriver::test4()
{
    cout << "\n  Test 4: Insert and scan variable-length records\n";
    Status status = OK;
    Scan* scan = 0;
    RID rid;

    cout << "  - Create a heap file\n";
    HeapFile f( "file_2", status );
    
    // cout << "# unpined buf = " << MINIBASE_BM->getNumUnpinnedBuffers() << endl; 
    // cout << "# bufs = " << MINIBASE_BM->getNumBuffers() << endl;
    
   if (status != OK)
        cerr << "*** Could not create heap file\n";
    else if ( MINIBASE_BM->getNumUnpinnedBuffers()
                != MINIBASE_BM->getNumBuffers() ) {
        cerr << "*** The heap file has left pages pinned\n";
        status = FAIL;
    }

   unsigned recSize = MINIBASE_PAGESIZE / 2;
   unsigned numRecs = 0;
   char record[MINIBASE_PAGESIZE] = "";
   if ( status == OK ) {
      cout << "  - Add variable-sized records\n";

      /* We use half a page as the starting size so that a single page won't
         hold more than one record.  We add a series of records at this size,
         then halve the size and add some more, and so on.  We store the index
         number of each record on the record itself. */
     
      for ( ; recSize >= (sizeof numRecs + sizeof recSize) && (status == OK);
            recSize /= 2 )
        for ( unsigned i=0; i < 10 && status == OK; ++i, ++numRecs )
          {
            memcpy( record, &numRecs, sizeof numRecs );
            memcpy( record+(sizeof numRecs), &recSize, sizeof recSize );
            status = f.insertRecord( record, recSize, rid );
            if ( status != OK ) {
                cerr << "*** Failed inserting record " << numRecs
                     << ", of size " << recSize << endl;
                break;
            }
            else 
	      cout << " inserting record " << numRecs << endl;
          }
   }

   int seen[numRecs];
   memset( seen, 0, sizeof seen );
   if ( status == OK ) {
     cout << "  - Check that all the records were inserted\n";
     scan = f.openScan(status);
     if (status != OK)
       cerr << "*** Error opening scan\n";
   }

   if ( status == OK ) {
     int len;
     while ( (status = scan->getNext(rid, record, len)) == OK ) {
       unsigned recNum;
       memcpy( &recNum, record, sizeof recNum );
       if ( seen[recNum] ) {
         cerr << "*** Found record " << recNum << " twice!\n";
         status = FAIL;
         break;
       }
       seen[recNum] = TRUE;

       memcpy( &recSize, record + sizeof recNum, sizeof recSize );
       if ( recSize != (unsigned)len ) {
         cerr << "*** Record size mismatch: stored " << recSize
              << ", but retrieved " << len << endl;
         status = FAIL;
         break;
       }
       cout << " seen record " << numRecs << endl;
       --numRecs;
    }

    if ( status == DONE ){
      if ( numRecs )
        cerr << "*** Scan missed " << numRecs << " records\n";
      else
        status = OK;
	}
   }

   delete scan;

   if ( status == OK )
     cout << "  Test 4 completed successfully.\n";
   return (status == OK);
}



int HeapDriver::test5()
{
    cout << "\n  Test 5: Test some error conditions\n";
    Status status = OK;
    Scan* scan = 0;
    RID rid;

    HeapFile f("file_1", status);
    if (status != OK)
        cerr << "*** Error opening heap file\n";

    if ( status == OK )
      {
        cout << "  - Try to change the size of a record\n";
        scan = f.openScan(status);
        if (status != OK)
            cerr << "*** Error opening scan\n";
      }
    if ( status == OK )
      {
        int len;
        Rec rec;
        status = scan->getNext(rid, (char *)&rec, len);
        // cout << "(" << rid.pageNo << "," << rid.slotNo << ")" << " : ";
        // cout << "ival = " << rec.ival << " fval = " << rec.fval << " name = " << rec.name << endl; 
        // cout << "status = " << status << endl;
        if ( status != OK )
            cerr << "*** Error reading first record\n";
        else
          {
            status = f.updateRecord( rid, (char*)&rec, len-1 );
            // cout << "@ after f.updateRec, status = " << status << endl;
            testFailure( status, HEAPFILE, "Shortening a record" );
            if ( status == OK )
              {
                status = f.updateRecord( rid, (char*)&rec, len+1 );
                testFailure( status, HEAPFILE, "Lengthening a record" );
              }
          }
      }

    delete scan;

    if ( status == OK )
      {
        cout << "  - Try to insert a record that's too long\n";
        char record[MINIBASE_PAGESIZE] = "";
        status = f.insertRecord( record, MINIBASE_PAGESIZE, rid );
        testFailure( status, HEAPFILE, "Inserting a too-long record" );
      }

    if ( status == OK )
        cout << "  Test 5 completed successfully.\n";
    return (status == OK);
}


int HeapDriver::test6()
{
    cout << "\n Test 6: Test delete file\n";
    Status status;

    HeapFile f("file_1", status);
    if (status != OK)
        cerr << "*** Error opening heap file\n";

    if (status == OK) {
      cout << "  Try to delete the heap file" << endl;
      status = f.deleteFile();
      if (status != OK)
	cerr << "*** Error deleting the heap file ***" << endl;
    }

    if (status == OK)
      cout << " Test 6 completed successfully" << endl;

    return (status == OK);
}
