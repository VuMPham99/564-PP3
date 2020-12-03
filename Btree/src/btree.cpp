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
		try {
			file = new BlobFile(outIndexName, false);
			//File Exists
			//TODO: remove this output once implementation is done
			std::cout << relationName + ":: File found!" << std::endl;	
			//get metadata
			Page* headerPage;
			bufMgr->readPage(file, file->getFirstPageNo(), headerPage);
			IndexMetaInfo* header = (IndexMetaInfo *) headerPage;
			rootPageNum = header->rootPageNo;
            bufMgr->unPinPage(file, file->getFirstPageNo(), false);
		} catch (FileNotFoundException &e) {
			//File Does Not Exist
			//set fields
			file = new BlobFile(outIndexName, true);
			//TODO: remove this output once implementation is done
			std::cout << relationName + ":: File not found!" << std::endl;
			//compute meta data 
            //allocate pages
			Page *headerPage;
			Page *rootPage;
			bufMgr->allocPage(file, rootPageNum, rootPage);
			bufMgr->allocPage(file, headerPageNum, headerPage);

			//create header with index meta data
			IndexMetaInfo* header = new IndexMetaInfo();
			strcpy(header->relationName, relationName.c_str());
			header->attrByteOffset = attrByteOffset;
			header-> attrType = attrType;
			header->rootPageNo = rootPageNum;	

            LeafNodeInt *root = (LeafNodeInt *) rootPage;
            root->rightSibPageNo = 0;
			root->level = 1;
			//std::cout << "Root page level is:: " << ((NonLeafNodeInt *)rootPage)->level << std::endl;	
			std::cout << relationName + ":: Allocated pages!" << std::endl;
			std::cout << relationName + ":: Unpinned pages!" << std::endl;
			//unpin pages
			bufMgr->unPinPage(file, headerPageNum, true);
    		bufMgr->unPinPage(file, rootPageNum, true);
			//TODO: scan relation
			FileScan* scanner = new FileScan(relationName, bufMgr);
			RecordId currRid;
			try {
				while(1) {
					scanner->scanNext(currRid);
					insertEntry(scanner->getRecord().c_str() + attrByteOffset, currRid);
				}	
			} catch (EndOfFileException &e) {
                //save file to disk
                bufMgr->flushFile(file);
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

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
	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	void BTreeIndex::startScan(const void *lowValParm,
							   const Operator lowOpParm,
							   const void *highValParm,
							   const Operator highOpParm)
	{
		std::cout << "Started Scanning! :D" << std::endl;
		//set range values
		highValInt = *((int *) highValParm);
		lowValInt = *((int *) lowValParm);

		//check for op code violation
		if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) 
		{
			throw new BadOpcodesException();
		}
		//check for range violation
		else if (lowValInt > highValInt)
		{
			throw new BadScanrangeException();
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
		NonLeafNodeInt* curr = (NonLeafNodeInt *) currentPageData;
		//if the root is not a leaf
		if (curr->level != 1) {
			//find the leaf node
			bool leafFound = false;
			while(!leafFound) 
			{
				//Find which page to go to
				PageId nextPageNum;
				findNextNonLeaf(curr, nextPageNum, lowValInt);
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = nextPageNum;
				//read the page
				bufMgr->readPage(file, currentPageNum, currentPageData);
				//cast page as a non leaf node
				curr = (NonLeafNodeInt *) currentPageData;
				//check if page is leaf
				if (curr->level == 1) {
					leafFound = true;
				}
			}
		}
		std::cout << "Leaf has been found!" << std::endl;
		//find first entry to scan
		bool entryFound = false;
		while (!entryFound) {
			//cast page to leaf node
			LeafNodeInt* curr = (LeafNodeInt *) currentPageData;
			//if entire page is empty, the key is not in the B+ tree
			if (curr->ridArray[0].page_number == 0)
			{
				std::cout << "Node is empty! :O" << std::endl;
				bufMgr->unPinPage(file, currentPageNum, false);
				throw new NoSuchKeyFoundException();
			}
			//else search node from left to right
			bool isNull = false;
			for (int i = 0; i < leafOccupancy && !isNull; i++)
			{
				std::cout << "Iterating over entry" << std::endl;
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
					throw new NoSuchKeyFoundException();
				}
				//if we've reached the end of the current leaf
				if (isNull || i == leafOccupancy)
				{
					bufMgr->unPinPage(file, currentPageNum, false);
					//if we've in the last leaf node, key is not in the B+ tree
					if (curr->rightSibPageNo == 0)
					{
						throw new NoSuchKeyFoundException();
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

	void BTreeIndex::scanNext(RecordId &outRid)
	{
		//check if scan has started yet
		if (!scanExecuting) {
			throw new ScanNotInitializedException();
		}
		LeafNodeInt* curr = (LeafNodeInt *) currentPageData;
		//if end of node is reached
		if (nextEntry == leafOccupancy || curr->ridArray[nextEntry].page_number == 0)
		{
			bufMgr->unPinPage(file, currentPageNum, false);
			//if we're in the last node, end scan
			if (curr->rightSibPageNo == 0) {
				endScan();
				throw new IndexScanCompletedException();
			}
			//otherwise go to next node
			nextEntry = 0;
			currentPageNum = curr->rightSibPageNo;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			curr = (LeafNodeInt *) currentPageData;
		}
		//check if current entry has key within range
		int key = curr->keyArray[nextEntry];
		if (isKeyValid(lowValInt, lowOp, highValInt, highOp, key))
		{
			outRid = curr->ridArray[nextEntry];
			nextEntry++;
		}
		//if not end the scan
		else
		{
			endScan();
			throw new IndexScanCompletedException();
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	void BTreeIndex::endScan()
	{
		if (!scanExecuting)
		{
			throw ScanNotInitializedException();
		}
		scanExecuting = false;
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageData = nullptr;
		currentPageNum = 0;
		nextEntry = 0;
	}

	/**
	 * TODO: Add comments
	 */
	void BTreeIndex::findNextNonLeaf(NonLeafNodeInt * curr, PageId &nextPageNum, int key)
	{
		int i = nodeOccupancy;
		while (i >= 0 && (curr->pageNoArray[i] == 0))
		{
			i--;
		}
		while (i > 0 && (curr->keyArray[i-1] >= key))
		{
			i--;
		}
		nextPageNum = curr->pageNoArray[i];
	}

	/**
	 * TODO: Add comments
	 */
	bool BTreeIndex::isKeyValid(int lowVal, Operator lowOp, int highVal, Operator highOp, int key)
	{
		std::cout << "checking validity of " + key << std::endl;
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
		else {
			return (key < highVal && key > lowVal);
		}
	}

} // namespace badgerdb
