/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

//#define DEBUG

namespace badgerdb
{

	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------
	/**
   * BTreeIndex Constructor. 
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
	BTreeIndex::BTreeIndex(const std::string &relationName,
						   std::string &outIndexName,
						   BufMgr *bufMgrIn,
						   const int attrByteOffset,
						   const Datatype attrType)
	{
		//set index fields
		bufMgr = bufMgrIn;
		attributeType = attrType;
		this->attrByteOffset = attrByteOffset;
		nodeOccupancy = INTARRAYNONLEAFSIZE;
		leafOccupancy = INTARRAYLEAFSIZE;
		scanExecuting = false;
		//compute indexName
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		outIndexName = idxStr.str();
		//Try opening the file
		try
		{
			file = new BlobFile(outIndexName, false);
			//File Exists
			//get metadata
			Page *headerPage;
			bufMgr->readPage(file, file->getFirstPageNo(), headerPage);
			IndexMetaInfo *header = (IndexMetaInfo *)headerPage;
			rootPageNum = header->rootPageNo;
			bufMgr->unPinPage(file, file->getFirstPageNo(), false);
		}
		catch (FileNotFoundException &e)
		{
			//File Does Not Exist
			//set fields
			file = new BlobFile(outIndexName, true);
            //allocate pages
			Page *headerPage;
			Page *rootPage;
			bufMgr->allocPage(file, headerPageNum, headerPage);
			bufMgr->allocPage(file, rootPageNum, rootPage);

			//create header with index meta data
			IndexMetaInfo *header = new IndexMetaInfo();
			strcpy(header->relationName, relationName.c_str());
			header->attrByteOffset = attrByteOffset;
			header->attrType = attrType;
			header->rootPageNo = rootPageNum;

			LeafNodeInt *root = (LeafNodeInt *)rootPage;
			root->rightSibPageNo = 0;
			root->level = 1;
			//unpin pages
			bufMgr->unPinPage(file, headerPageNum, true);
			bufMgr->unPinPage(file, rootPageNum, true);
			//TODO: scan relation
			FileScan *scanner = new FileScan(relationName, bufMgr);
			RecordId currRid;
			try
			{
				while (1)
				{
					scanner->scanNext(currRid);
					insertEntry(scanner->getRecord().c_str() + attrByteOffset, currRid);
				}
			}
			catch (EndOfFileException &e)
			{
				//save file to disk
				bufMgr->flushFile(file);
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	/**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	BTreeIndex::~BTreeIndex()
	{
		scanExecuting = false;
		bufMgr->flushFile(BTreeIndex::file);
		delete file;
		file = nullptr;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------
	/**
	 * Insert a new entry using the pair <value,rid>. 
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
	void BTreeIndex::insertEntry(const void *key, const RecordId rid)
	{	
		RIDKeyPair<int> entry;
		entry.set(rid, *((int *) key));
		//set current page number & data to root
		bufMgr->readPage(file, rootPageNum, currentPageData);
		currentPageNum = rootPageNum;
        bool isLeaf = false;
		PageKeyPair<int> *newEntry = nullptr;
        if (((NonLeafNodeInt *)currentPageData)->level == 1) {

            isLeaf = true;
        }
		//call helper
		insertHelper(currentPageData, currentPageNum, entry, newEntry, isLeaf);
	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::insertHelper
	// -----------------------------------------------------------------------------

    void BTreeIndex::insertHelper(Page * currentPage, PageId currentPageId, RIDKeyPair<int> entry, PageKeyPair<int> *&newEntry, bool isLeaf)
    {
		//if node is a leaf, insert new entry
        if (isLeaf)
        {
			LeafNodeInt *curr = (LeafNodeInt *) currentPage;
            //if page is not at capacity, insert into it
            if (curr->ridArray[leafOccupancy - 1].page_number == 0)
            {
				curr->level = 1;
                insertLeaf(curr, entry);
                bufMgr->unPinPage(file, currentPageId, true);
                newEntry = nullptr;
            }
            //else, split
            else {
                splitLeaf(curr, currentPageId, newEntry, entry);
            }
        }
		//else, go to the correct child
        else
        {
            NonLeafNodeInt *curr = (NonLeafNodeInt *) currentPage;
            PageId nextPageNum;
            Page *nextPage;
            findNextNonLeaf(curr, nextPageNum, entry.key);
            bufMgr->readPage(file, nextPageNum, nextPage);
			NonLeafNodeInt * nextNode = (NonLeafNodeInt *) nextPage;
			if (nextNode->level == 1)
			{
				isLeaf = true;
			}
            insertHelper(nextPage, nextPageNum, entry, newEntry, isLeaf);
            //if there has been no split in the child node, unpin the page
            if (newEntry == nullptr)
            {
                bufMgr->unPinPage(file, currentPageId, false);
            }
            else
            {
				//std::cout << "This page is a leaf!" << std::endl;
                //if the currentPage is not at capacity, insert into it
                if (curr->pageNoArray[nodeOccupancy] == 0)
                {
                    insertNonLeaf(curr, newEntry);
                    newEntry = nullptr;
                    bufMgr->unPinPage(file, currentPageId, true);
                }
                //else split the non leaf node  
                else {
                    splitNonLeaf(curr, currentPageId, newEntry);
                } 
            } 
        }
    }
    
	// -----------------------------------------------------------------------------
	// BTreeIndex::splitNonLeaf
	// -----------------------------------------------------------------------------

    void BTreeIndex::splitNonLeaf(NonLeafNodeInt *currNode, PageId currPageId, PageKeyPair<int> *&newEntry)
    {
        //allocate the new right node
        Page * newPage;
        PageId newPageId;
        PageKeyPair<int> newParentEntry;
        bufMgr->allocPage(file, newPageId, newPage);
        NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;
        newNode->level = 0;
        int midIndex = nodeOccupancy/2;
        int newParentIndex;
        //if capacity is even,
        if (nodeOccupancy%2 == 0)
        {
            if (newEntry->key < currNode->keyArray[midIndex])
            {
                newParentIndex = midIndex - 1;
            }
            else 
            {
                newParentIndex = midIndex;
            }
        }
        midIndex = newParentIndex + 1;
        newParentEntry.set(newPageId, currNode->keyArray[newParentIndex]);
        //fill the split nodes with half the entries
        for (int i = midIndex; i < nodeOccupancy; i++)
        {
            int newIndex = i - midIndex;
            //fill the right (new) node & remove those entries from the left (curr) node
            newNode->keyArray[newIndex] = currNode->keyArray[i];
			currNode->keyArray[i+1] = 0;
            newNode->pageNoArray[newIndex] = currNode->pageNoArray[i+1];
            currNode->pageNoArray[i+1] = 0;
        }
        //remove new parent entry from the left node
        currNode->pageNoArray[newParentIndex] = 0;
        currNode->keyArray[newParentIndex] = 0;
        NonLeafNodeInt * newParent;
        //if the new parent entry belongs in the left node
        if (newEntry->key < newNode->keyArray[0])
        {
            newParent = currNode;
        }
        else {
            newParent = newNode;
        }
        insertNonLeaf(newParent, newEntry);
        newEntry = &newParentEntry;
        //write pages to the disk
        bufMgr->unPinPage(file, newPageId, true);
        bufMgr->unPinPage(file, currPageId, true);
        //if we're splitting the root, call updateRoot
        if (currPageId == rootPageNum)
        {
            rootUpdater(currPageId, newEntry);
        }
    }

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertLeaf
	// -----------------------------------------------------------------------------

	void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> entry)
	{
		// leaf page is not empty
		if(leaf->ridArray[0].page_number != 0) {
			int last = leafOccupancy - 1;

			for(int i = last; i >= 0; i--) {
				if(leaf->ridArray[i].page_number == 0) {
					last--;
				}
			}

			for(int i = last; i >= 0; i--) {
				if(entry.key < leaf->keyArray[i]) {
					leaf->keyArray[last+1] = leaf->keyArray[last];
      				leaf->ridArray[last+1] = leaf->ridArray[last];
					last--;
				}
			}	
			
			leaf->ridArray[last + 1] = entry.rid;
			leaf->keyArray[last + 1] = entry.key;	
		}	
		// leaf page is empty
		else {
			
			// insert the leaf
			leaf->ridArray[0] = entry.rid;
			leaf->keyArray[0] = entry.key;
			
		}
	}
	
	// -----------------------------------------------------------------------------
	// BTreeIndex::insertNonLeaf
	// -----------------------------------------------------------------------------

	void BTreeIndex::insertNonLeaf(NonLeafNodeInt *nonleaf, PageKeyPair<int> *entry) {
		int last = nodeOccupancy;

		for(int i = last; i >= 0; i--) {
			if(nonleaf->pageNoArray[last] == 0) {
				last--;
			}
		}
		
		for(int i = last; i >= 0; i--) {
			if(nonleaf->keyArray[last-1] > entry->key) {
				nonleaf->keyArray[last] = nonleaf->keyArray[last-1];
    			nonleaf->pageNoArray[last+1] = nonleaf->pageNoArray[last];
    			last--;
			}
		}	
				
		// insert nonleaf
		nonleaf->keyArray[last] = entry->key;
		nonleaf->pageNoArray[last+1] = entry->pageNo;		
	}
	
	// -----------------------------------------------------------------------------
	// BTreeIndex::rootUpdater
	// -----------------------------------------------------------------------------

	void BTreeIndex::rootUpdater(PageId firstRootPage, PageKeyPair<int> *newEntry)
	{
		Page *root;
		PageId rootId;
		bufMgr->allocPage(file, rootId, root);
		NonLeafNodeInt *newRoot = (NonLeafNodeInt *)root;
		
		//Set key & pointers
		newRoot->keyArray[0] = newEntry->key;
		newRoot->pageNoArray[0] = firstRootPage;
		newRoot->pageNoArray[1] = newEntry->pageNo;
		newRoot->level = 0;

		//update root page number
		Page *mPage;
		bufMgr->readPage(file, headerPageNum, mPage);
		IndexMetaInfo *meta = (IndexMetaInfo *)mPage;
		meta->rootPageNo = rootId;
		rootPageNum = rootId;

		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootId, true);
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::splitLeaf
	// -----------------------------------------------------------------------------

	void BTreeIndex::splitLeaf(LeafNodeInt *leaf, PageId leafPId, PageKeyPair<int> *&newEntry, RIDKeyPair<int> entry)
	{
		Page *page;
		PageId pageNum;
		bufMgr->allocPage(file, pageNum, page);
		LeafNodeInt *newLeaf = (LeafNodeInt *)page;
		newLeaf->level = 1;
		newEntry = new PageKeyPair<int>();
		PageKeyPair<int> newPair;

		int center = leafOccupancy/2;
		if(entry.key > leaf->keyArray[center] && leafOccupancy % 2 == 1){
			center++;
		}
		for(int i = center; i < leafOccupancy; i++){
			newLeaf->ridArray[i-center] = leaf->ridArray[i];
			leaf->ridArray[i].page_number = 0;
			newLeaf->keyArray[i-center] = leaf->keyArray[i];
			leaf->keyArray[i] = 0;
		}

		newLeaf->rightSibPageNo = leaf->rightSibPageNo;
		leaf->rightSibPageNo = pageNum;

		if(entry.key > leaf->keyArray[center-1]){
			insertLeaf(newLeaf, entry);
		}
		else{
			insertLeaf(leaf, entry);
		}
		newPair.set(pageNum, newLeaf->keyArray[0]);
		newEntry = &newPair;
		bufMgr->unPinPage(file, pageNum, true);
		bufMgr->unPinPage(file, leafPId, true);
		if(rootPageNum == leafPId){
			rootUpdater(leafPId, newEntry);
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------
	/**
	 * Begin a filtered scan of the index.  For instance, if the method is called 
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	void BTreeIndex::startScan(const void *lowValParm,
							   const Operator lowOpParm,
							   const void *highValParm,
							   const Operator highOpParm)
	{
		//set range values
		lowValInt = *((int *) lowValParm);
		highValInt = *((int *) highValParm);

		//check for op code violation
		if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE))
		{
			throw BadOpcodesException();
		}
		//check for range violation
		else if (highValInt < lowValInt)
		{
			throw BadScanrangeException();
		}
		//set operator values
		highOp = highOpParm;
		lowOp = lowOpParm;
		//end scan if it has already started
		if (scanExecuting)
		{
			endScan();
		}
		//start scan
		currentPageNum = rootPageNum;
		bufMgr->readPage(file, currentPageNum, currentPageData);
		NonLeafNodeInt *curr = (NonLeafNodeInt *)currentPageData;
		//if the root is not a leaf
		if (curr->level != 1)
		{
			//find the leaf node
			bool leafFound = false;
			while (!leafFound)
			{
				//Find which page to go to
				PageId nextPageNum;
				findNextNonLeaf(curr, nextPageNum, lowValInt);
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = nextPageNum;
				//read the page
				bufMgr->readPage(file, currentPageNum, currentPageData);
				//cast page as a non leaf node
				curr = (NonLeafNodeInt *)currentPageData;
				//check if page is leaf
				if (curr->level == 1)
				{
					leafFound = true;
				}
			}
		}
		//find first entry to scan
		bool entryFound = false;
		while (!entryFound)
		{
			//cast page to leaf node
			LeafNodeInt *curr = (LeafNodeInt *)currentPageData;
			//if entire page is empty, the key is not in the B+ tree
			if (curr->ridArray[0].page_number == 0)
			{
				bufMgr->unPinPage(file, currentPageNum, false);
				throw NoSuchKeyFoundException();
			}
			//else search node from left to right
			bool isNull = false;
			for (int i = 0; i < leafOccupancy && !isNull; i++)
			{
				//if current entry is null, set isNull to true
				if (i < leafOccupancy - 1 && curr->ridArray[i + 1].page_number == 0)
				{
					isNull = true;
				}
				//check if current entry's key is within range
				int key = curr->keyArray[i];
				if (isKeyValid(lowValInt, lowOp, highValInt, highOp, key))
				{
					//starting entry has been found
					entryFound = true;
					scanExecuting = true;
					nextEntry = i;
					break;
				}
				//check operator edge cases
				else if ((highOp == LTE && key > highValInt) || (highOp == LT && key >= highValInt))
				{
					bufMgr->unPinPage(file, currentPageNum, false);
					throw NoSuchKeyFoundException();
				}
				//if we've reached the end of the current leaf
				if (isNull || i == leafOccupancy)
				{
					bufMgr->unPinPage(file, currentPageNum, false);
					//if we've in the last leaf node, key is not in the B+ tree
					if (curr->rightSibPageNo == 0)
					{
						throw NoSuchKeyFoundException();
					}
					//otherwise, go to next node
					currentPageNum = curr->rightSibPageNo;
					bufMgr->readPage(file, currentPageNum, currentPageData);
				}
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------
	 /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	void BTreeIndex::scanNext(RecordId &outRid)
	{
		//check if scan has started yet
		if (scanExecuting == false) {
			throw ScanNotInitializedException();

		}
		LeafNodeInt *curr = (LeafNodeInt *)currentPageData;
		//if end of node is reached
		if (nextEntry == leafOccupancy || curr->ridArray[nextEntry].page_number == 0)
		{
			bufMgr->unPinPage(file, currentPageNum, false);
			//if we're in the last node, end scan
			if (curr->rightSibPageNo == 0) {
				throw IndexScanCompletedException();

			}
			//otherwise go to next node
			nextEntry = 0;
			currentPageNum = curr->rightSibPageNo;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			curr = (LeafNodeInt *)currentPageData;
		}
		//check if current entry has key within range
		int key = curr->keyArray[nextEntry];
		bool isValid = isKeyValid(lowValInt, lowOp, highValInt, highOp, key);
		if (isValid)
		{
			outRid = curr->ridArray[nextEntry];
			nextEntry++;
		}
		//if not end the scan
		else
		{
			throw IndexScanCompletedException();
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------

	void BTreeIndex::endScan()
	{
		if (scanExecuting == false)
		{
			throw ScanNotInitializedException();
		}
		nextEntry = -1;
		scanExecuting = false;
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = -1;
		currentPageData = nullptr;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::findNextNonLeaf
	// -----------------------------------------------------------------------------

	void BTreeIndex::findNextNonLeaf(NonLeafNodeInt * curr, PageId &nextPageNum, int key)
	{
		int last = nodeOccupancy;
		for (int i = last; i >= 0; i--) {
			if (curr->pageNoArray[last] == 0)
			{
				last--;
			}
		}
		for (int i = last; i > 0; i--)
		{
			if (curr->keyArray[last-1] >= key)
			{
				last--;
			}
		}
		nextPageNum = curr->pageNoArray[last];
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::isKeyValid
	// -----------------------------------------------------------------------------
	
	bool BTreeIndex::isKeyValid(int lowVal, Operator lowOp, int highVal, Operator highOp, int key)
	{
		//if operators are '<=' & '>='
		if (highOp == LTE && lowOp == GTE)
		{
			return (key <= highVal && key >= lowVal);
		}
		//if operators are '<=' & '>'
		else if (highOp == LTE && lowOp == GT)
		{
			return (key <= highVal && key > lowVal);
		}
		//if operators are '<' & '>='
		else if (highOp == LT && lowOp == GTE)
		{
			return (key < highVal && key >= lowVal);
		}
		//if operators are '<' & '>'
		else
		{
			return (key < highVal && key > lowVal);
		}
	}

} // namespace badgerdb
