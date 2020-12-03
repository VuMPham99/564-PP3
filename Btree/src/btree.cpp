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
			IndexMetaInfo* header = new IndexMetaInfo();
			strcpy(header->relationName, relationName.c_str());
			header->attrByteOffset = attrByteOffset;
			header-> attrType = attrType;
			header->rootPageNo = 2;
			//create header with index meta data
			Page* headerPage;
			bufMgr->allocPage(file, headerPageNum, headerPage);
			headerPage = (Page *) header; 
            //create root page
            Page* rootPage;
			bufMgr->allocPage(file, rootPageNum, rootPage);
            LeafNodeInt *root = new LeafNodeInt();
            root->rightSibPageNo = 0;
			rootPage = (Page *) root;

			std::cout << relationName + ":: Allocated pages!" << std::endl;
			//unpin pages
			bufMgr->unPinPage(file, headerPageNum, true);
    		bufMgr->unPinPage(file, rootPageNum, true);
			std::cout << relationName + ":: Unpinned pages!" << std::endl;
			//TODO: scan relation
			FileScan* scanner = new FileScan(relationName, bufMgr);
			RecordId currRid;
			try {
				while(1) {
					scanner->scanNext(currRid);
					insertEntry(scanner->getRecord().c_str() + attrByteOffset, currRid);
				}	
			} catch (EndOfFileException &e) {
				std::cout << relationName + ":: Done Scanning! :D" << std::endl;
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
        //searchTree(key, rid, rootPageNum);
		//RIDKeyPair<int> entry;
		//PageKeyPair<int> *newEntry = nullptr;
		//Page* root = rootPageNum;
		//entry.set(rid, *((int *)key));
		//bufMgr->readPage(file, rootPageNum, root);
		
	}

    // /**
    //  * 
    //  **/
    // void BTreeIndex::searchTree(const void *key, const RecordId rid, PageId currentNodeNum) {
    //     // Page currPage = file->readPage(currentNodeNum);
    //     // if ((LeafNodeInt *)currPage->level == 1) {
    //     //     LeafNodeInt* node = (LeafNodeInt *) currPage;

    //     // }
    //     // else {
    //     //     NonLeafNodeInt* node = (NonLeafNodeInt *) currPage;
    //     //     int keys = node->keyArray;
    //     //     int i = 0;
    //     //     //iterate through keyArray until we find a key that is greater than the new key
    //     //     while(*key < keys[i]) {

    //     //     }
    //     //     if(){
				
	// 	// 	}
    //     // }
    // }


	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	void BTreeIndex::startScan(const void *lowValParm,
							   const Operator lowOpParm,
							   const void *highValParm,
							   const Operator highOpParm)
	{
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	void BTreeIndex::scanNext(RecordId &outRid)
	{
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

} // namespace badgerdb
