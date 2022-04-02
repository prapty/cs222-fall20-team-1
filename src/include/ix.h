#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        const size_t TYPE_OF_NODE = PAGE_SIZE - sizeof(short) - 2*sizeof(int);
        const size_t AVAILABLE_SPACE = PAGE_SIZE - 2*sizeof(short) - 2*sizeof(int);
        const size_t NEXT_PAGE = PAGE_SIZE - 3*sizeof(short) - 2*sizeof(int);
        const size_t ixSLOT_SIZE = 2*sizeof(int);
        const size_t ENTRY_POINTER = sizeof(int);
        const size_t RID_SIZE = 2*sizeof(int);
        int rootPage = -1;
        int rootPagePointer=0;

        RC copyFileHandle(FileHandle &fileHandle, IXFileHandle &ixFileHandle, int direction) const;

        int insertKey(FileHandle &fileHandle, const Attribute &attribute, int page, const void *key, const RID &rid, void* newChildEntry, int& isSplit);

        int deleteKey(FileHandle &fileHandle, const Attribute &attribute, int page, const void *key, const RID &rid, void* oldChildEntry);

        RC insertNodeEntry(FileHandle &fileHandle, void* inputData, const Attribute &attribute, const void *key, const RID &rid, int rightNumPage, int leftNumPage);

        void initializePage(void* targetPage, short typeOfPage);

        RC deleteNodeEntry(FileHandle &fileHandle, void* inputData, int slotNum);

        RC splitLeafPage(FileHandle &fileHandle, void* targetPage, const Attribute &attribute, int page, const void *key, const RID &rid, void* newPage, void* upperEntry);
        RC splitInternalPage(FileHandle &fileHandle, void* targetPage, const Attribute &attribute, int page, const void *keyData, const RID &rid, void* newPage, void* upperEntry, int keyRightPage);

        int findKey(void* nodeData, const Attribute &attribute, const void *key, const RID &rid, int& slotNum);

        int findKeyUptoLeaf(FileHandle &fileHandle, int page, const Attribute &attribute, const void *key, const RID &rid, int& slotNum);

        int compareKey(const void* data, const void* key, const Attribute &attribute);

        string printTreeString(FileHandle &fileHandle, const Attribute &attribute, int page) const;

        void printHelper(FileHandle &fileHandle, int pageNum);

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        //add more variable to match with pfm
        unsigned ixNumberOfPages;
        unsigned ixNumberOfHiddenPages;
        //unsigned pfmDataEndPos=5*sizeof(unsigned);
        unsigned ixDataEndPos;
        FILE * ixF_pointer;
        std::string ixFileName;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    };

    class IX_ScanIterator {
    public:
        FileHandle fileHandle;
        IXFileHandle ixFileHandle;
        Attribute attribute;

        int currentPageNum;
        int currentSlotNum;
        int currentPageNumOfRecords;
        char *currentPageData=new char[PAGE_SIZE];
        int currentPageIterationNo;

        const void *lowKey;
        const void *highKey;

        bool lowKeyInclusive;
        bool highKeyInclusive;

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        int compareKey(const void* data, const void* key, const Attribute &attribute);

        RC copyFileHandle(FileHandle &fileHandle, IXFileHandle &ixFileHandle, int direction) const;
    };

}// namespace PeterDB
#endif // _ix_h_
