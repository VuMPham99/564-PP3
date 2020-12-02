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
		PageId headerPageNumber;
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
			bufMgr->readPage(file, 1, headerPage);
			IndexMetaInfo* header = (IndexMetaInfo *) headerPage;
			rootPageNum = header->rootPageNo;
		} catch (FileNotFoundException &e) {
			//File Does Not Exist
			//set fields
			file = new BlobFile(outIndexName, true);
			rootPageNum = 2;
			//TODO: remove this output once implementation is done
			std::cout << relationName + ":: File not found!" << std::endl;
			//compute meta data
			IndexMetaInfo* header = new IndexMetaInfo();
			strcpy(header->relationName, relationName.c_str());
			header->attrByteOffset = attrByteOffset;
			header-> attrType = attrType;
			header->rootPageNo = 2;
			//create header with index meta data
			Page* headerPage = (Page *) header; 
			//add meta data as first page to the file
			bufMgr->allocPage(file, headerPageNumber, headerPage);
			//set the header page number
			headerPageNum = headerPageNumber;
			//TODO: scan relation
			FileScan* scanner = new FileScan(relationName, bufMgr);
			RecordId currRid;
			int currKey;
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
