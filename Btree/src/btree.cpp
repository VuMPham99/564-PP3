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
		scanExecuting = false;
		bufMgr = bufMgrIn;

//compute indexName
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		std::string indexName = idxStr.str();
		outIndexName = indexName;
		//compute meta data
		IndexMetaInfo* header = new IndexMetaInfo();
		//header->relationName = relationName;
		header->attrByteOffset = attrByteOffset;
		header-> attrType = attrType;
		//Try opening the file
		try {
			file = new BlobFile(indexName, false);
			//File Exists
			std::cout << relationName + ":: File found!" << std::endl;			

		} catch (FileNotFoundException e) {
			//File Does Not Exist
			file = new BlobFile(indexName, true);
			std::cout << relationName + ":: File not found!" << std::endl;
			file = new BlobFile(outIndexName, true);

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
		RIDKeyPair<int> entry;
		PageKeyPair<int> *newEntry = nullptr;
		Page* root;
		entry.set(rid, *((int *)key));
		bufMgr->readPage(file, rootPageNum, root);
	}

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
