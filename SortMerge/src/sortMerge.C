
#include <string.h>
#include <assert.h>
#include "sortMerge.h"

#define CHECK_RETURN_STATUS if (status != OK) {returnStatus = status; return;}

// Error Protocall:

#define x(q) printf("%d\n", q);


enum ErrCodes {SORT_FAILED, HEAPFILE_FAILED};

static const char* ErrMsgs[] =  {
  "Error: Sort Failed.",
  "Error: HeapFile Failed."
  // maybe more ...
};

static error_string_table ErrTable( JOINS, ErrMsgs );

void printEntry(char *recPtr, int rightp, int row_num, int len_in, AttrType in[], short t_str_sizes[], int join_col) {
	printf("%s row %d", rightp? "right " : "left  ", row_num);
	int offset = 0;
	for(int i = 0; i < len_in; i++) {
		if(join_col == i)
			printf(" [[");
		else
			printf(" [");
		if(in[i] == attrInteger)
			printf("%d", *((int*)recPtr+offset));
		else
			printf("%s", (recPtr+offset));
		if(join_col == i)
			printf("]]");
		else
			printf("]");
		offset += t_str_sizes[i];	
	}
	printf("\n");
}

void printRID(RID r) {
	printf("<%d, %d> ", r.pageNo, r.slotNo);
}


// sortMerge constructor
sortMerge::sortMerge(
    char*           filename1,      // Name of heapfile for relation R
    int             len_in1,        // # of columns in R.
    AttrType        in1[],          // Array containing field types of R.
    short           t1_str_sizes[], // Array containing size of columns in R
    int             join_col_in1,   // The join column of R 

    char*           filename2,      // Name of heapfile for relation S
    int             len_in2,        // # of columns in S.
    AttrType        in2[],          // Array containing field types of S.
    short           t2_str_sizes[], // Array containing size of columns in S
    int             join_col_in2,   // The join column of S

    char*           filename3,      // Name of heapfile for merged results
    int             amt_of_mem,     // Number of pages available
    TupleOrder      order,          // Sorting order: Ascending or Descending
    Status&         returnStatus          // Status of constructor
){
	setbuf(stdout, NULL);
	//printf("Merging records of lengths %d and %d\n", len_in1, len_in2);
	returnStatus = OK;

	// SETUP
	// * open heapfiles 1 and 2 for reading
	// * construct heapfile 3
	// * sort heapfiles 1 and 2 creating heapfiles 1a and 2a
	// * open heapfiles 1a and 2a
	// * open Scans scan1 and scan2 on heapfile1a and 2a
	// * entry1 = scan1.getFirst()
	// * entry2 = scan2.getFirst()
	// * hf1done = hf2done = 0

	Status status;
	
	char* sorted_filename_1 = (char*) malloc(strlen(filename1)+8);
	char* sorted_filename_2 = (char*) malloc(strlen(filename2)+8);
	sprintf(sorted_filename_1, "%s%s", filename1, "_sorted");
	sprintf(sorted_filename_2, "%s%s", filename2, "_sorted");
	//printf("Original files: %s %s\n", filename1, filename2);
	//printf("Sorted files: %s %s\n", sorted_filename_1, sorted_filename_2);

	Sort *sort1 = new Sort(filename1, sorted_filename_1, len_in1, in1, t1_str_sizes, join_col_in1,order, amt_of_mem, status); ; CHECK_RETURN_STATUS
	Sort *sort2 = new Sort(filename2, sorted_filename_2, len_in2, in2, t2_str_sizes, join_col_in2,order, amt_of_mem, status); ; CHECK_RETURN_STATUS
	delete sort1;
	delete sort2;
	//printf("** Done sorting, resulted in files %s and %s **\n\n", sorted_filename_1, sorted_filename_2);

	//printf("** Creating heapfiles for sort-merge: %s + %s -> %s **\n", sorted_filename_1, sorted_filename_2, filename3);
	HeapFile *hf1 = new HeapFile(sorted_filename_1, status);  CHECK_RETURN_STATUS
	HeapFile *hf2 = new HeapFile(sorted_filename_2, status);   CHECK_RETURN_STATUS
	HeapFile *hf3 = new HeapFile(filename3, status); CHECK_RETURN_STATUS

	//printf("** Opening scans on %s and %s **\n", sorted_filename_1, sorted_filename_2);
	Scan *scan1 = hf1->openScan(status); CHECK_RETURN_STATUS
	Scan *scan2 = hf2->openScan(status); CHECK_RETURN_STATUS

	RID leftRID, rightRID;
	char* recPtrLeft, *recPtrRight;

	int recLenLeft= 0;
	int recLenRight= 0;
	for(int i = 0; i < len_in1; i++)
		recLenLeft += t1_str_sizes[i];
	for(int i = 0; i < len_in2; i++)
		recLenRight += t2_str_sizes[i];

	recPtrLeft = (char*) malloc(recLenLeft * sizeof(char));
	recPtrRight = (char*) malloc(recLenRight * sizeof(char));

	int dummy;
	// get first entries 
	status = scan1->getNext(leftRID, recPtrLeft, dummy); CHECK_RETURN_STATUS
	assert(dummy == recLenLeft);
	status = scan2->getNext(rightRID, recPtrRight, dummy); CHECK_RETURN_STATUS
	assert(dummy == recLenRight);

	int done = 0;
	
	// set up to use tupleCmp in the loop
	int cmp;

	int join_loc_left, join_loc_right;
	join_loc_left = join_loc_right = 0;
	for(int i = 0; i < join_col_in1; i++)
		join_loc_left += t1_str_sizes[i];
	for(int i = 0; i < join_col_in2; i++)
		join_loc_right += t1_str_sizes[i];

	// not defined, but they say it works on Piazza
	// key_size = t1_str_sizes[join_col_in1];  // I don't like this
	key1_pos = join_loc_left;
	key2_pos = join_loc_right;

	char* oldRecPtrRight = (char*) malloc(recLenRight * sizeof(char));

	RID oldRightRID;

	int i = 0;
	int j = 0;
	int j_return = 0;
	RID markerRID;
	int iterLeft, iterRight;
	while(!done) {
		// cmp = sort1->tupleCmp(recPtrLeft, recPtrRight);
		//printRID(leftRID); printf(" vs " ); printRID(rightRID);printf("\n");
		////printEntry(recPtrLeft, 0, i, len_in1, in1, t1_str_sizes, join_col_in1);
		////printEntry(recPtrRight, 1, j, len_in2, in2, t2_str_sizes, join_col_in2);
		cmp = tupleCmp(recPtrLeft, recPtrRight);
		if(cmp == 0) {
			// printf("** match **\n");
			markerRID = rightRID;
			//printf("mark: "); printRID(markerRID); printf("\n");
			j_return = j;
			iterLeft = iterRight = 1;
			while(iterLeft) {
				iterRight = 1;
				while(iterRight) {
					memcpy(oldRecPtrRight, recPtrRight, recLenRight * sizeof(char));
					oldRightRID = rightRID;
					int len_out;
					RID dummy_RID;

					char *newRecPtr = tupleUnion(recPtrLeft, len_in1, recLenLeft, recPtrRight, len_in2, t2_str_sizes, join_col_in2, len_out);
					hf3->insertRecord(newRecPtr, len_out, dummy_RID);

					status = scan2->getNext(rightRID, recPtrRight, dummy); 
					j++;
					if(status == DONE) {
						done = 1;
						iterLeft = iterRight = 0;
						break;
					} else {
						 CHECK_RETURN_STATUS
					}
					cmp = tupleCmp(recPtrLeft, recPtrRight);
					if(cmp != 0) {
						iterRight = 0;
					}
				}
				if(status == DONE) 
					break;
				status = scan1->getNext(leftRID, recPtrLeft, dummy); 
				i++;
				if(status == DONE) {
					iterLeft = iterRight = 0;
					break;
				} else {
					 CHECK_RETURN_STATUS
				}
				cmp = tupleCmp(recPtrLeft, oldRecPtrRight);
				if(cmp != 0) {
					iterLeft = 0;
				} else {
					scan2->position(markerRID);
					status = scan2->getNext(rightRID, recPtrRight, dummy); 
					j = j_return;
				}
			}
		} else if(cmp < 0)  {
			status = scan1->getNext(leftRID, recPtrLeft, dummy);
			i++;
			if(status == DONE) {
				break;
			} else {
				 CHECK_RETURN_STATUS
			}
		} else {
			//printf("Incrementing right column\n");
			status = scan2->getNext(rightRID, recPtrRight, dummy);
			j++;
			if(status == DONE) {
				break;
			} else {
				 CHECK_RETURN_STATUS
			}
		}
	}  // hf1done or hf2done
	delete scan1;
	delete scan2;
	delete hf2;
	delete hf1;
	delete hf3;
	// delete sort1;
	// delete sort2;
	
	returnStatus = OK;
}
	// LOOP
	// while(!hf1done and !hf2done)
	//   cmp = sort1.tupleCmp(entry1[jc1], entry2[jc2])
	//   if(cmp == 0) {
	//     markerRID = entry2.RID
	//        iterLeft = iterRight = 1
	//        while iterLeft
	//          iterRight = 1
	//          scan2.position(markerRID)
	//          while iterRight
	//            oldEntry2 = entry2 
	//            hf3.insertRecord(join(entry1, entry2))
	//            entry2 = scan2getNext()
	//            cmp = sort1.tupleCmp(entry1[jc1], entry2[jc2])
	//              if(cmp != 0)
	//                iterRight = 0
	//          entry1 = scan1.getNext()
	//          cmp = sort1.tupleCmp(entry1, oldEntry2)
	//          if(cmp != 0)
	//            iterLeft = 0
	// methods
	// from sort.h:
	//    Sort(char* infile, char* outfile,
	//         int len_in, AttrType in[], short str_sizes[], int fld_no,
	//         TupleOrder sort_order, int amt_of_bif, Status &s)
	//    int tupleCmp(const void *t1, const void *t2)
	// 
	// from sortMerge.h
	//     error codes SORT_FAILED HEAPFILE_FAILED
	// 
	//  from hfpage.h
	//    insertRecord(), deleteRecord()
	//
	//  from scan.h
	//    mvNext(RID &rid)
	//    position(RID &rid)
	//
	//  from heapfile.h
	//    insertRecord(char *recPtr, int recLen, RID &outRid)
	//    getRecord(const RID &rid, char *recPtr, int& recLen)
	//    openScan(Status &status)

// sortMerge destructor
sortMerge::~sortMerge()
{
	//printf("Destroying sortMerge object\n");
	// fill in the body
}

char* sortMerge::tupleUnion(char *recPtr1, int len_in1, int block1_len, // block1_len = recLenLeft;
												 char *recPtr2, int len_in2, short t2_str_sizes[], int join_col_in2, 
												 int &len_out
												 ){

	// assume columns from R, including join column in position in R,
	// then columns from S, excluding join column
	// also assume columns are properly padded

	int block2a_len = 0;
	int block2b_len = 0;
	
  //	*out = (AttrType*) malloc((len_in1+len_in2-1)*sizeof(AttrType));

	// for(int i = 0; i < len_in1; j++) {
	//	 block1_len += t1_str_sizes[i];
  //	 out[i] = in1[i];				
	// }
	for(int i = 0; i < join_col_in2; i++) {
		block2a_len += t2_str_sizes[i];
		//out[i+len_in1] = in2[i];
	}
	for(int i = 1+join_col_in2; i < len_in2; i++) {
		block2b_len += t2_str_sizes[i];
		//out[i+len_in1-1] = in2[i];
	}

	len_out = block1_len + block2a_len + t2_str_sizes[join_col_in2] + block2b_len;
	// len_out = block1_len + block2a_len + block2b_len;

	char *recPtrOut = (char*) malloc(len_out*sizeof(char));
	memcpy(recPtrOut,                        recPtr1,                                        block1_len);
	memcpy(recPtrOut+block1_len,             recPtr2,                                        block2a_len + t2_str_sizes[join_col_in2] + block2b_len);
	//memcpy(recPtrOut+block1_len+block2a_len, recPtr2+block2a_len+t2_str_sizes[join_col_in2], block2b_len);

	return recPtrOut;
}

