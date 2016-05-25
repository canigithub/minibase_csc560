/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
  if (t == attrString) {
      return strcmp((char *)key1, (char *)key2);
  } else if (t == attrInteger) {
      return (*(int *)key1 - *(int *)key2);
  } else {
      cerr << "wrong attrType" << endl;
      exit(1);
  }
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
    int key_size = 0;
    int data_size = 0;
   
    if (key_type == attrString) {
        key_size = strlen((char *)key);
    } else if (key_type == attrInteger){
        key_size = sizeof(int);
    }
    if ((key_size % sizeof(PageId)) != 0) {
        key_size += (sizeof(PageId) - (key_size % sizeof(PageId)));
    }
    if (ndtype == INDEX) {
        data_size = sizeof(PageId);
    } else if (ndtype == LEAF) {
        data_size = sizeof(RID);
    }
    
		
    *pentry_len = key_size + data_size;
    memcpy(target, key, key_size);
    memcpy((char*)target+key_size, &data, data_size);
		
		/*
		Keytype *key__ = (Keytype*) malloc(sizeof(Keytype));
		Datatype *data__ = (Datatype*) malloc(sizeof(Datatype));
		get_key_data(key__, data__, target, *pentry_len, ndtype);
		printf("get_key_data thinks we made entry with key =  ");
		if(key_type == attrString)
			printf("%s ", key__->charkey);
		else
			printf("%d ", key__->intkey);
		printf(" and data ");
		if(ndtype == LEAF)
			printf("<%d,%d>\n", data__->rid.pageNo, data__->rid.slotNo);
		else
			printf("%d\n", data__->pageNo);
*/
     
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
    int key_size = 0;
    int data_size = 0;
    if (ndtype == INDEX) {
        data_size = sizeof(PageId);
    } else if (ndtype == LEAF) {
        data_size = sizeof(RID);
    }
    key_size = entry_len - data_size;
    memcpy(targetkey, psource, key_size);
    memcpy(targetdata, (char*)psource+key_size, data_size);
 
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
    int key_size = 0;
    if (key_type == attrString) {
        key_size = strlen((char *)key);
    } else if (key_type == attrInteger) {
        key_size = sizeof(int);
    } else {
        cerr << "Wrong attrType" << endl;
        exit(1);
    }
    return key_size;
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
    int key_size = get_key_length(key, key_type);
    int data_size = 0;
    if (ndtype == INDEX) {
        data_size = sizeof(PageId);
    } else if (ndtype == LEAF) {
        data_size = sizeof(RID);
    }
    return (key_size+data_size);
}
