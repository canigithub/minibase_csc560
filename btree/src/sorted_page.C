/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  //OK,
  //Insert Record Failed (SortedPage::insertRecord),
  //Delete Record Failed (SortedPage::deleteRecord,
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

void SortedPage::printAllRecords() {
	KeyDataEntry *kde = (KeyDataEntry*) malloc(sizeof(KeyDataEntry));
	int offset;
	int length;
	Keytype *key_ = (Keytype*) malloc(sizeof(Keytype));
	Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
	printf("Contents of sorted page (if LEAF):\n");
	for(int i = 0; i < slotCnt; i++) {
		offset = slot[i].offset;
		length = slot[i].length;
		memcpy(kde, &data[offset], length);
		get_key_data(key_, data_, kde, length, LEAF);
		printf("slot %2d at offset %d with length %d: <%d, [%d,%d]>\n",
				i, slot[i].offset, slot[i].length, key_->intkey, data_->rid.pageNo, data_->rid.slotNo);
	}
	printf("Contents of sorted page (if INDEX):\n");
	for(int i = 0; i < slotCnt; i++) {
		offset = slot[i].offset;
		length = slot[i].length;
		memcpy(kde, &data[offset], length);
		get_key_data(key_, data_, kde, length, INDEX);
		printf("slot %2d at offset %d with length %d: <%d, %d>\n",i, slot[i].offset, slot[i].length, key_->intkey, data_->pageNo);
	}
	free(key_);
	free(kde);
	free(data_);
}

Status SortedPage::insertRecord (AttrType key_type,
                                 char * recPtr,
                                 int recLen,
                                 RID& rid)
{
		printf("Entered SortedPage::insertRecord\n");
		printAllRecords();
    int i; // using i to iterate the slots
    int j;
    int offset = usedPtr-recLen;
    
    if (available_space() < recLen)
        return DONE;
    
    for (i = 0; i < slotCnt; ++i) {
        if (keyCompare(recPtr, &data[slot[i].offset], key_type) > 0) {
            continue;


				// WELCOME TO THE ACTION
        } else {
            offset = slot[i].offset;
            // update the offset for slots after slot_n
            for (j = i; j < slotCnt; ++j) {
                slot[j].offset -= recLen;
            }
            memmove(&slot[i+1], &slot[i], sizeof(slot_t)*(slotCnt-i));
						/*printf("Let's look at slot %d\n", i);
						printf("offset: %d\n", slot[i].offset);
						printf("length: %d\n", slot[i].length);
						printf("usedPtr = %d\n", usedPtr);
						printf("recLen = %d\n", recLen);
						printf("Copying offset+length-usedPtr = %d bytes from %ld to %ld\n", 
								offset+slot[i].length-usedPtr,
								(unsigned long) data+usedPtr,
								(unsigned long) data+usedPtr-recLen);
           */
					 memmove(data+usedPtr-recLen, data+usedPtr, offset+slot[i+1].length-usedPtr);
            break;
        }
    }
    
    rid.pageNo = curPage;
    rid.slotNo = i;

		/*
		Keytype *key_ = (Keytype*) malloc(sizeof(Keytype));
		Datatype *data_ = (Datatype*) malloc(sizeof(Datatype));
		*/
    slot[i].offset = offset;
    slot[i].length = recLen;
    ++slotCnt;
    usedPtr -= recLen;
    freeSpace -= (recLen + sizeof(slot_t));
		
		// **** this is the point of contention ****
    memmove(&data[offset], recPtr, recLen);
		/*
		printf("just memmoved %d bytes\n", recLen);
		for(int i = 0 ; i < 1+recLen/4; i++) {
			printf("recPtr[%d]:      %d\n", i*4, *(i+(int*)recPtr));
			printf("data[offset+%d]: %d\n", i*4, *(i+(int*)&data[offset])); 
		}
		*/

/*
		printf("** original data with recPtr **\n");
		get_key_data(key_, data_, (KeyDataEntry*) recPtr, slot[i].length, LEAF);
		printf("if leaf:  slot %d   offset %d   length %d   key %d and data [%d,%d]\n", 
				i, slot[i].offset, slot[i].length, key_->intkey, data_->rid.pageNo, data_->rid.slotNo);
		get_key_data(key_, data_, (KeyDataEntry*) recPtr, slot[i].length, INDEX);
		printf("if index: slot %d  offset %d  length %d   key %d   data %d\n", 
				i, slot[i].offset, slot[i].length, key_->intkey, data_->pageNo);

		printf("** stored data at &data[slot[i].offset] **\n");
		get_key_data(key_, data_, (KeyDataEntry*) &data[slot[i].offset], slot[i].length, LEAF);
		printf("if leaf:  slot %d   offset %d   length %d   key %d and data [%d,%d]\n", 
				i, slot[i].offset, slot[i].length, key_->intkey, data_->rid.pageNo, data_->rid.slotNo);
		get_key_data(key_, data_, (KeyDataEntry*) &data[slot[i].offset], slot[i].length, INDEX);
		printf("if index: slot %d  offset %d  length %d   key %d   data %d\n", 
				i, slot[i].offset, slot[i].length, key_->intkey, data_->pageNo);

		printf("Now have usedPtr = %d and freeSpace = %d\n", usedPtr, freeSpace);

    printAllRecords();
		*/
    return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
    int i;
    int slot_n = rid.slotNo;
    if (slot_n >= slotCnt)
        return FAIL;
    
    slot_t *curr = slot + slot_n;
    int recLen = curr->length;
    int offset = curr->offset;
    memmove(data+usedPtr+recLen, data+usedPtr, offset-usedPtr);
    
    // update the offset for slots after slot_n
    for (i = slot_n+1; i < slotCnt; ++i) {
        slot[i].offset += recLen;
    }
    
    memmove(&slot[slot_n], &slot[slot_n+1], sizeof(slot_t));
    
    --slotCnt;
    usedPtr += recLen;
    freeSpace += (recLen + sizeof(slot_t));
		printf("Deleted a record, have usedPtr = %d and freeSpace = %d\n", usedPtr, freeSpace);
    return OK;
}

int SortedPage::numberOfRecords()
{
  return slotCnt;
}
