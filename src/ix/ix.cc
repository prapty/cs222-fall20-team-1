#include <sys/stat.h>
#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    PagedFileManager *pagedFileManager = &PagedFileManager::instance();

    RC IndexManager::createFile(const std::string &fileName) {
        //destroyFile(fileName);
        return pagedFileManager->createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return pagedFileManager->destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        struct stat stat_buffer;
        if(stat(fileName.c_str(), &stat_buffer) != 0) {
            //fileName does not exist return -1
            return -1;
        }
        if(ixFileHandle.ixF_pointer!=NULL){
            return -1;
        }
        FileHandle fileHandle;
        int returnValue = pagedFileManager->openFile(fileName, fileHandle);
        if (returnValue == 0) {
            //to copy data from fileHandle to ixFileHandle
            copyFileHandle(fileHandle, ixFileHandle, 0);
            return 0;
        } else {
            return -1;
        }
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        FileHandle fileHandle;
        copyFileHandle(fileHandle, ixFileHandle, 1);

        if(ixFileHandle.ixNumberOfPages!=0){
            //writing root page num in root page pointer
            char *data = new char[PAGE_SIZE];
            memset(data, 0, PAGE_SIZE);
            fileHandle.readPage(rootPagePointer, data);
            memcpy((char *) data, &rootPage, sizeof(int));
            fileHandle.writePage(rootPagePointer, data);
        }

        pagedFileManager->closeFile(fileHandle);

        ixFileHandle.ixReadPageCounter = 0;
        ixFileHandle.ixWritePageCounter = 0;
        ixFileHandle.ixAppendPageCounter = 0;
        ixFileHandle.ixNumberOfPages=0;
        ixFileHandle.ixF_pointer = NULL;
        rootPage = -1;
        //free(data);
        return 0;
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        FileHandle fileHandle;
        copyFileHandle(fileHandle, ixFileHandle, 1);
        //initial stage, no root created yet
        if(ixFileHandle.ixNumberOfPages==0){
            //printf("First entry, create root pointer page and root page\n");
            //create root pointer page. We are not allowed to store in hidden page
            char* rootPinterData = new char[PAGE_SIZE];
            memset(rootPinterData, 0, PAGE_SIZE);
            int futureRoot=fileHandle.numberOfPages+1;
            memcpy(rootPinterData, &futureRoot, sizeof(int));
            fileHandle.appendPage(rootPinterData);

            //add the root+leaf page into the file
            char* newPageData = new char[PAGE_SIZE];
            memset(newPageData, 0, PAGE_SIZE);
            //root page is initially a leaf page
            initializePage(newPageData, 2);
            fileHandle.appendPage(newPageData);
            //printHelper(fileHandle,fileHandle.numberOfPages-1);
            rootPage=fileHandle.numberOfPages-1;
            //printf("rootPage in insert %d\n", rootPage);
        }
            //read root page num from root pointer page
        else{
            char *data = new char[PAGE_SIZE];
            memset(data, 0, PAGE_SIZE);
            fileHandle.readPage(rootPagePointer, data);
            memcpy(&rootPage, (char *) data, sizeof(int));
            //free(data);
        }
        char* newChildEntry = new char[PAGE_SIZE];
        int isSplit = 0;
        // call insertKey
        int result = insertKey(fileHandle, attribute, rootPage, key, rid, newChildEntry, isSplit);
        //printf("File: %s, result: %d\n", ixFileHandle.ixFileName.c_str(), result);
        copyFileHandle(fileHandle, ixFileHandle, 0);
        if(result >= 0) return 0;
        else return -1;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        FileHandle fileHandle;
        copyFileHandle(fileHandle, ixFileHandle, 1);
        //always read root page instead of using global variable
        char *data = new char[PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        fileHandle.readPage(rootPagePointer, data);
        memcpy(&rootPage, (char *) data, sizeof(int ));
        int result = deleteKey(fileHandle, attribute, rootPage, key, rid, nullptr);
        copyFileHandle(fileHandle, ixFileHandle, 0);
        //free(data);
        if(result >= 0) return 0;
        else return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        struct stat stat_buffer;
        if(stat(ixFileHandle.ixFileName.c_str(), &stat_buffer) != 0) {
            //fileName does not exist return -1
            return -1;
        }

        FileHandle fileHandle;
        copyFileHandle(fileHandle, ixFileHandle, 1);

        char* data = new char[PAGE_SIZE];
        int result = fileHandle.readPage(rootPagePointer, data);
        if(result==-1){
            return -1;
        }

        memcpy(&rootPage, (char *) data, sizeof(int ));

        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.lowKeyInclusive=lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive=highKeyInclusive;
        int slotNum=0;
        RID iRID;
        int pageNum=findKeyUptoLeaf(fileHandle, rootPage, attribute, lowKey, iRID, slotNum);
        copyFileHandle(fileHandle, ixFileHandle, 0);
        ix_ScanIterator.currentPageNum=pageNum;
        ix_ScanIterator.currentSlotNum=slotNum;
        ix_ScanIterator.fileHandle = fileHandle;
        ix_ScanIterator.ixFileHandle=ixFileHandle;

        //ix_ScanIterator.currentPageData=malloc(PAGE_SIZE);
        memset(ix_ScanIterator.currentPageData, 0, PAGE_SIZE);
        ix_ScanIterator.currentPageIterationNo=0;

        ix_ScanIterator.lowKey=lowKey;
        ix_ScanIterator.highKey=highKey;
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        FileHandle fileHandle;
        copyFileHandle(fileHandle, ixFileHandle, 1);
        out << printTreeString(fileHandle, attribute, rootPage) + "\n";
        return 0;
    }

    string IndexManager::printTreeString(FileHandle &fileHandle, const Attribute &attribute, int page) const {
        //check if this node is leaf node or none-leaf node
        char *targetPage = new char[PAGE_SIZE];
        if(fileHandle.readPage(page, targetPage) != 0)
            return "ERROR";
        short isLeaf;
        memcpy(&isLeaf, (char*)targetPage + TYPE_OF_NODE, sizeof(short));
        //printf("RESULT %d %d %d\n", *(int*) ((char*)targetPage + 4),*(short*) ((char*)targetPage + 8), *(short*) ((char*)targetPage + 10));
        int numberOfEntry;
        memcpy(&numberOfEntry, (char*) targetPage + NUM_RECORD, sizeof(int));
        string treeString = "{\"keys\":[";
        int endOfPageOffset = NEXT_PAGE;
        //if this is an non-leaf node (0 for root, 1 for internal node)
        if (isLeaf == 0 ||isLeaf == 1){
            string children = 	"\"children\":[";
            int next_page;
            //for the first pointer
            memcpy(&next_page, (char*) targetPage, sizeof(int));
            //start to recursively run the function for its children
            children += printTreeString(fileHandle, attribute, next_page);
            children += ",\n";
            for(int i = 0; i < numberOfEntry; i++){
                int current_offset;
                int current_length;
                endOfPageOffset -= ixSLOT_SIZE;
                memcpy(&current_offset, (char *) targetPage + endOfPageOffset, sizeof(int));
                memcpy(&current_length, (char *) targetPage + endOfPageOffset + sizeof(int), sizeof(int));
                if(attribute.type == TypeVarChar){
                    int length;
                    memcpy(&length, (char*) targetPage + current_offset, sizeof(int));
                    char* charValue = new char[length];
                    memcpy(charValue, (char*) targetPage + current_offset + sizeof(int), length);
                    //get the pageNum to the next page
                    memcpy(&next_page, (char*) targetPage + current_offset + sizeof(int) + length + RID_SIZE, sizeof(int));
                    string varcharValue;
                    for (int j = 0; j < length; j++) {
                        varcharValue += charValue[j];
                    }
                    treeString+= "\"" + varcharValue + "\"";
                }
                else if(attribute.type == TypeReal){
                    float value;
                    memcpy(&value, (char*) targetPage + current_offset, sizeof(float));
                    treeString+= "\"" + to_string(value) + "\"";
                    //get the pageNum to the next page
                    memcpy(&next_page, (char*) targetPage + current_offset + sizeof(int) + RID_SIZE, sizeof(int));
                }
                else if(attribute.type == TypeInt){
                    int value;
                    memcpy(&value, (char*) targetPage + current_offset, sizeof(int));
                    treeString+= "\"" + to_string(value) + "\"";
                    //get the pageNum to the next page
                    memcpy(&next_page, (char*) targetPage + current_offset + sizeof(int) + RID_SIZE, sizeof(int));
                }
                //start to recursively run the function for its children
                children += printTreeString(fileHandle, attribute, next_page);
                if(i + 1 < numberOfEntry) {
                    treeString += ",";
                    children += ",\n";
                }
            }
            treeString += "],\n" + children + "\n]}";
        }
            //if this is a leaf node, from the example we can have same key but different RID ex. {"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]}
        else if (isLeaf == 2){
            char* temp = new char[attribute.length + 4]; // in case for varchar
            float floatTemp = 0;
            int intTemp = 0;
            for(int i = 0; i < numberOfEntry; i++){
                int current_offset;
                int current_length;
                //for rid
                int pageNum;
                int slotNum;
                endOfPageOffset -= ixSLOT_SIZE;
                memcpy(&current_offset, (char *) targetPage + endOfPageOffset, sizeof(int));
                memcpy(&current_length, (char *) targetPage + endOfPageOffset + sizeof(int), sizeof(int));
                if(attribute.type == TypeVarChar){
                    int length;
                    memcpy(&length, (char*) targetPage + current_offset, sizeof(int));
                    char* charValue = new char[length];
                    memcpy(charValue, (char*) targetPage + current_offset + sizeof(int), length);
                    // read pageNum and slotNum
                    memcpy(&pageNum, (char*) targetPage + current_offset + sizeof(int) + length, sizeof(int));
                    memcpy(&slotNum, (char*) targetPage + current_offset + sizeof(int) + length + sizeof(int), sizeof(int));
                    string varcharValue;
                    for (int j = 0; j < length; j++) {
                        varcharValue += charValue[j];
                    }
                    if(memcmp(charValue, temp, length) == 0 && i != 0){
                        treeString += ",";
                    }
                    else{
                        //treeString += "]}";
                        if(i + 1 < numberOfEntry){
                            treeString += ",\n";
                        }
                        treeString += "\"" + varcharValue + ":[";
                        memcpy(temp, charValue, length);
                    }
                    treeString += "(" + to_string(pageNum) +"," + to_string(slotNum) + ")";
                }
                    //if it is real/int
                else {
                    // read pageNum and slotNum
                    memcpy(&pageNum, (char*) targetPage + current_offset + sizeof(int) , sizeof(int));
                    memcpy(&slotNum, (char*) targetPage + current_offset + 2*sizeof(int), sizeof(int));
                    if (attribute.type == TypeReal) {
                        float value;
                        memcpy(&value, (char *) targetPage + current_offset, sizeof(float));
                        if(floatTemp == value && i != 0){
                            treeString += ",";
                        }
                        else{
                            //treeString += "]}";
                            if(i + 1 < numberOfEntry){
                                treeString += ",\n";
                            }
                            treeString += "\"" + to_string(value) + ":[";
                            floatTemp = value;
                        }
                        treeString += "(" + to_string(pageNum) +"," + to_string(slotNum) + ")";
                    } else if (attribute.type == TypeInt) {
                        int value;
                        memcpy(&value, (char *) targetPage + current_offset, sizeof(int));
                        if(intTemp == value && i != 0){
                            treeString += ",";
                        }
                        else{
                            //treeString += "]}";
                            if(i + 1 < numberOfEntry){
                                treeString += ",\n";
                            }
                            treeString += "\"" + to_string(value) + ":[";
                            intTemp = value;
                        }
                        treeString += "(" + to_string(pageNum) +"," + to_string(slotNum) + ")";
                    }
                }
            }
            if(numberOfEntry > 0) treeString += "]\"";
            treeString += "]}\n";
        }
      //  printf("\n%s\n", treeString.c_str());
        return treeString;
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        int NEXT_PAGE=PAGE_SIZE - 3*sizeof(short) - 2*sizeof(int);
        int ixSLOT_SIZE=2*sizeof(int);
        const size_t ENTRY_POINTER = sizeof(int);
        const size_t RID_SIZE = 2*sizeof(int);
        while (currentPageNum>0){
            if(currentPageIterationNo==0) {
                if (fileHandle.readPage(currentPageNum, currentPageData) != 0) {
                    copyFileHandle(fileHandle, ixFileHandle, 0);
                    return IX_EOF;
                }
            }
                bool lowConditionFulfilled= false;
                bool highConditionFulfiled=false;

                memcpy(&currentPageNumOfRecords, (char *)currentPageData+NUM_RECORD, sizeof(int));
                int iSlotLocation= NEXT_PAGE - (currentSlotNum+1)*ixSLOT_SIZE;
                int currentOffset;
                memcpy(&currentOffset, (char*) currentPageData + iSlotLocation, sizeof(int));
                int currentLength;
                memcpy(&currentLength, (char*) currentPageData + iSlotLocation + sizeof(int), sizeof(int));
                int keyLength= currentLength-RID_SIZE-ENTRY_POINTER;
                char *keyData = new char[keyLength];
                memcpy(keyData, (char*) currentPageData + currentOffset, keyLength);
                //printf("GENEXTENTRY 1\n");
                if(lowKey== nullptr){
                    lowConditionFulfilled=true;
                }
                else{
                    //int compareWithLow=compareKey(keyData, lowKey, attribute);
                    int compareWithLow=compareKey(lowKey, keyData, attribute);
                    if(compareWithLow>0){
                        lowConditionFulfilled=true;
                    }
                    else if(lowKeyInclusive && compareWithLow==0){
                        lowConditionFulfilled=true;
                    }
                }
                if(highKey== nullptr){
                    highConditionFulfiled=true;
                }
                else{
                    //int compareWithHigh=compareKey(keyData, highKey, attribute);
                    int compareWithHigh=compareKey(highKey, keyData, attribute);
                    if(compareWithHigh<0){
                        highConditionFulfiled=true;
                    }
                    else if(highKeyInclusive && compareWithHigh==0){
                        highConditionFulfiled=true;
                    }
                }
                currentSlotNum+=1;
                currentPageIterationNo+=1;
                if(currentSlotNum==currentPageNumOfRecords){
                    short nextPage;
                    memcpy(&nextPage, (char *)currentPageData+NEXT_PAGE, sizeof(short ));
                    currentPageNum=nextPage;
                    currentSlotNum=0;
                    currentPageIterationNo=0;
                }
                if(lowConditionFulfilled && highConditionFulfiled){
                    memcpy(key, keyData, keyLength);
                    memcpy(&rid.pageNum, (char*) currentPageData + currentOffset + keyLength, sizeof(int));
                    memcpy(&rid.slotNum, (char*) currentPageData + currentOffset + keyLength + sizeof(int), sizeof(int));
                    copyFileHandle(fileHandle, ixFileHandle, 0);
                    return 0;
                }
            }
        copyFileHandle(fileHandle, ixFileHandle, 0);
        return IX_EOF;
    }

    int IX_ScanIterator::compareKey(const void* data, const void* key, const Attribute &attribute){
        if(attribute.type == TypeVarChar){
            int dataLength;
            int keyLength;
            memcpy(&dataLength, data, sizeof(int));
            memcpy(&keyLength, key, sizeof(int));
            string d((char *)data + sizeof(int), dataLength);
            string k((char *)key + sizeof(int), keyLength);
            int result = k.compare(d);
            if(result == 0) return 0;
            else if(result < 0) return -1;
            else
                return 1;
        }
        else if(attribute.type == TypeInt){
            int d;
            int k;
            memcpy(&d, data, sizeof(int));
            memcpy(&k, key, sizeof(int));
            if(d == k)
                return 0;
            else if(d > k)
                return -1;
            else
                return 1;
        }
        else{
            //for TypeReal
            float d;
            float k;
            memcpy(&d, data, sizeof(float));
            memcpy(&k, key, sizeof(float));
            if(d == k)
                return 0;
            else if(d > k)
                return -1;
            else
                return 1;
        }
    }

    RC IX_ScanIterator::copyFileHandle(FileHandle &fileHandle, IXFileHandle &ixFileHandle, int direction) const {
        if(direction == 0) {
            ixFileHandle.ixReadPageCounter = fileHandle.readPageCounter;
            ixFileHandle.ixWritePageCounter = fileHandle.writePageCounter;
            ixFileHandle.ixAppendPageCounter = fileHandle.appendPageCounter;
            ixFileHandle.ixNumberOfPages = fileHandle.numberOfPages;
            ixFileHandle.ixNumberOfHiddenPages = fileHandle.numberOfHiddenPages;
            ixFileHandle.ixF_pointer = fileHandle.F_pointer;
            ixFileHandle.ixFileName = fileHandle.fileName;
            ixFileHandle.ixDataEndPos = fileHandle.pfmDataEndPos;
        }
        else{
            fileHandle.readPageCounter = ixFileHandle.ixReadPageCounter;
            fileHandle.writePageCounter = ixFileHandle.ixWritePageCounter;
            fileHandle.appendPageCounter = ixFileHandle.ixAppendPageCounter;
            fileHandle.numberOfPages = ixFileHandle.ixNumberOfPages;
            fileHandle.numberOfHiddenPages = ixFileHandle.ixNumberOfHiddenPages;
            fileHandle.F_pointer = ixFileHandle.ixF_pointer;
            fileHandle.fileName = ixFileHandle.ixFileName;
            fileHandle.pfmDataEndPos = ixFileHandle.ixDataEndPos;
        }
    }

    RC IX_ScanIterator::close() {
        //free(currentPageData);
        copyFileHandle(fileHandle, ixFileHandle, 0);

        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        ixNumberOfPages = 0;
        //File pointer
        ixF_pointer = NULL;
        ixFileName = " ";
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount=ixReadPageCounter;
        writePageCount=ixWritePageCounter;
        appendPageCount=ixAppendPageCounter;
        return 0;
    }
    /*
     * Use to copy FileHandle data into IXFileHandle, if int direction = 0 then from fileHandle to ixFileHandle
     * reverse if direction != 0
     */
    RC IndexManager::copyFileHandle(FileHandle &fileHandle, IXFileHandle &ixFileHandle, int direction) const {
        if(direction == 0) {
            ixFileHandle.ixReadPageCounter = fileHandle.readPageCounter;
            ixFileHandle.ixWritePageCounter = fileHandle.writePageCounter;
            ixFileHandle.ixAppendPageCounter = fileHandle.appendPageCounter;
            ixFileHandle.ixNumberOfPages = fileHandle.numberOfPages;
            ixFileHandle.ixNumberOfHiddenPages = fileHandle.numberOfHiddenPages;
            ixFileHandle.ixF_pointer = fileHandle.F_pointer;
            ixFileHandle.ixFileName = fileHandle.fileName;
            ixFileHandle.ixDataEndPos = fileHandle.pfmDataEndPos;
        }
        else{
            fileHandle.readPageCounter = ixFileHandle.ixReadPageCounter;
            fileHandle.writePageCounter = ixFileHandle.ixWritePageCounter;
            fileHandle.appendPageCounter = ixFileHandle.ixAppendPageCounter;
            fileHandle.numberOfPages = ixFileHandle.ixNumberOfPages;
            fileHandle.numberOfHiddenPages = ixFileHandle.ixNumberOfHiddenPages;
            fileHandle.F_pointer = ixFileHandle.ixF_pointer;
            fileHandle.fileName = ixFileHandle.ixFileName;
            fileHandle.pfmDataEndPos = ixFileHandle.ixDataEndPos;
        }
    }

    int IndexManager::insertKey(FileHandle &fileHandle, const Attribute &attribute, int page, const void *key, const RID &rid, void* newChildEntry, int& isSplit){
        //check if this node is leaf node or none-leaf node
        char *targetPage = new char[PAGE_SIZE];
        if(fileHandle.readPage(page, targetPage) != 0)
            return -1;
        //printf("page %d key %d pageNum %d slotNum %d\n", page, *(int*)key, rid.pageNum, rid.slotNum);
        if(*(int*)key==56&&rid.pageNum==5&&rid.slotNum==47){
            //printf("key: %d pageNum %d slotNum %d\n", *(int*)key, rid.pageNum, rid.slotNum);
            //printHelper(fileHandle, page);
        }

       // printHelper(fileHandle, 5);
        short isLeaf;
        memcpy(&isLeaf, (char*)targetPage + TYPE_OF_NODE, sizeof(short));
        short pageAvailability;
        memcpy(&pageAvailability, (char*) targetPage + AVAILABLE_SPACE, sizeof(short));
        size_t attrLength = sizeof(int);
        int slotNum;
        if(attribute.type == TypeVarChar){
            int actualLength;
            memcpy(&actualLength, key, sizeof(int));
            attrLength+=actualLength;
        }
        //default value for leaf node
        int leftPageNum=-1;
        //if this is an non-leaf node (0 for root, 1 for internal node)
        if (isLeaf == 0 || isLeaf == 1){
//            if(page==3){
//                int leftPageNum;
//                memcpy(&leftPageNum, targetPage, ENTRY_POINTER);
//                int pageThreeNumRecords;
//                memcpy(&pageThreeNumRecords, (char *)targetPage+NUM_RECORD, sizeof(int));
//                printf("Left Pointer of page 3: %d, number of entries=%d\n",leftPageNum, pageThreeNumRecords);
//            }
            if(isLeaf==0){
                int leftPointer;
                memcpy(&leftPointer,targetPage,ENTRY_POINTER);
                //printf("Left pointer in root page: %d\n",leftPointer);
            }
            int newPageNum = findKey(targetPage, attribute, key, rid, slotNum);
            int result = insertKey(fileHandle, attribute, newPageNum, key, rid, newChildEntry, isSplit);
            //if isSplit != 0 (there is a split)
            if(isSplit != 0) {
                int newChildOffset=0;

                int keyLength;
                memcpy(&keyLength, (char *)newChildEntry+newChildOffset, sizeof(int));
                newChildOffset+=sizeof(int);

                memcpy(&leftPageNum, (char *)newChildEntry+newChildOffset, ENTRY_POINTER);
                newChildOffset+=ENTRY_POINTER;

                char* keyData = new char[keyLength];
                memcpy(keyData, (char*) newChildEntry + newChildOffset, keyLength);
                newChildOffset+=keyLength;
                //printf("keylength/leftPagnum %d  %d\n", keyLength, leftPageNum);
                RID newRid;
                memcpy(&newRid.pageNum, (char*) newChildEntry + newChildOffset, sizeof(int));
                newChildOffset+=sizeof(int);
                memcpy(&newRid.slotNum, (char*) newChildEntry + newChildOffset, sizeof(int));
                newChildOffset+=sizeof(int);
                //printf("RID %d  %d\n",newRid.pageNum, newRid.slotNum);
                int rightPageNum;
                memcpy(&rightPageNum, (char *)newChildEntry+newChildOffset, ENTRY_POINTER);
                //printf("newChildEntry != null %d\n", pageAvailability);
                //check if the current node has a space (so we can link the new leaf page into this one)
                if(pageAvailability > attrLength + RID_SIZE + ixSLOT_SIZE + ENTRY_POINTER){
                    int numberOfEntry;
                    memcpy(&numberOfEntry, (char*) targetPage + NUM_RECORD, sizeof(int));
                    //not split in internal so set it to zero
                    isSplit = 0;

                    //printf("INSERTX %d %d %d\n", rightPageNum, rootPage, page);
                    insertNodeEntry(fileHandle, targetPage, attribute, keyData, newRid, rightPageNum, leftPageNum);

                    int parentLeft;
                    memcpy(&parentLeft, targetPage, ENTRY_POINTER);
                    fileHandle.writePage(page, targetPage);
                    newChildEntry = nullptr;
                }
                    //not enough space need to split internal/root node
                else{
                    //printf("SPLIT INTERNAL!\n");

                    char *newPage = new char[PAGE_SIZE];
                    initializePage(newPage, 1);
                    isSplit = 1;
                    splitInternalPage(fileHandle, targetPage, attribute, page, keyData, newRid, newPage, newChildEntry, rightPageNum);
                    //check if this is the root node or not (if we need to split root node -> create new root node)
                    if(page == rootPage){
                        //change old root page into internal node
                        short interNode = 1;
                        memcpy((char*)targetPage + TYPE_OF_NODE, &interNode, sizeof(short));

                        char *newRootPage = new char[PAGE_SIZE];
                        initializePage(newRootPage, 0);

                        int newChildOffset=0;

                        int keyLength;
                        memcpy(&keyLength, (char *)newChildEntry+newChildOffset, sizeof(int));
                        newChildOffset+=sizeof(int);

                        memcpy(&leftPageNum, (char *)newChildEntry+newChildOffset, ENTRY_POINTER);
                        newChildOffset+=ENTRY_POINTER;

                        char* keyData = new char[keyLength];
                        memcpy(keyData, (char*) newChildEntry + newChildOffset, keyLength);
                        newChildOffset+=keyLength;
                        RID newRid;
                        memcpy(&newRid.pageNum, (char*) newChildEntry + newChildOffset, sizeof(int));
                        newChildOffset+=sizeof(int);
                        memcpy(&newRid.slotNum, (char*) newChildEntry + newChildOffset, sizeof(int));
                        newChildOffset+=sizeof(int);

                        int rightPageNum;
                        memcpy(&rightPageNum, (char *)newChildEntry+newChildOffset, ENTRY_POINTER);

                        //printf("keyData %d %d %d\n", *(int*)keyData, keyLength, rightPageNum);

                        //append new node to file
                        insertNodeEntry(fileHandle, newRootPage, attribute, keyData, newRid, rightPageNum, leftPageNum);
                        //append new node to file
                        fileHandle.appendPage(newRootPage);
                        int newRootPageNum=fileHandle.numberOfPages-1;
                        //save new root page id to file
                        char* rootPinterData = new char[PAGE_SIZE];
                        memset(rootPinterData, 0, PAGE_SIZE);
                        memcpy(rootPinterData, &newRootPageNum, sizeof(int));
                        fileHandle.writePage(rootPagePointer,rootPinterData);
                    }
                }
                return 0;
            }
            else {
                return result;
            }
        }
            //if this is a leaf node
        else if (isLeaf == 2){
            //if there is enough space
            if(pageAvailability > attrLength + RID_SIZE + ixSLOT_SIZE + ENTRY_POINTER){
                newChildEntry = nullptr;
                if(insertNodeEntry(fileHandle, targetPage, attribute, key, rid, -1, -1) == -1)
                    return -1;
                fileHandle.writePage(page, targetPage);
                return 0;
            }
                //not enough space need to split
            else{
                char *newPage = new char[PAGE_SIZE];
                initializePage(newPage, 2);
                isSplit = 1;
                splitLeafPage(fileHandle, targetPage, attribute, page, key, rid, newPage, newChildEntry);
                return 0;
            }
        }
        else
            return -1;
    }
    /*
     * a helper function that will delete key from the page with specific slot and move data
     * not finish yet
     */
    RC IndexManager::deleteNodeEntry(FileHandle &fileHandle, void* inputData, int slotNum){
        //this function will delete that slot and move every slot after it to the right by one
        if (inputData == nullptr)
            return -1;
        //deal with the end of the file
        int numberOfRecord;
        char* outputData = new char[PAGE_SIZE];
        memcpy(&numberOfRecord, (char *) inputData + NUM_RECORD, sizeof(int));
        memcpy((char*) outputData + TYPE_OF_NODE, (char*) inputData + TYPE_OF_NODE, sizeof(short));
        memcpy((char*) outputData + NEXT_PAGE, (char*) inputData + NEXT_PAGE, sizeof(short));
        int endPageOffset_in = NEXT_PAGE;
        int endPageOffset_out = NEXT_PAGE;
        int latestOffset = 0;
        int length = 0;
        bool isExist = false;
        int targetOffset;
        memcpy(&targetOffset, (char*) inputData + NEXT_PAGE - ixSLOT_SIZE*(slotNum + 1), sizeof(int));
        memcpy(&length, (char*) inputData + NEXT_PAGE - ixSLOT_SIZE*(slotNum + 1) + sizeof(int), sizeof(int));
        for (int i = 0; i < numberOfRecord; i++) {
            endPageOffset_in -= (int)ixSLOT_SIZE;
            endPageOffset_out -= (int)ixSLOT_SIZE;
            if (i == slotNum) {
                //read the length to move stuff around
                endPageOffset_out += (int)ixSLOT_SIZE;
                isExist = true;
            }
            else{
                int current_offset;
                int current_length;
                memcpy(&current_offset, (char *) inputData + endPageOffset_in, sizeof(int));
                memcpy(&current_length, (char *) inputData + endPageOffset_in + sizeof(int), sizeof(int));
                if(targetOffset > current_offset){
                    memcpy((char *) outputData + current_offset, (char *) inputData + current_offset, current_length);
                    memcpy((char *) outputData + endPageOffset_out, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset_out + sizeof(int), &current_length, sizeof(int));
                    if (latestOffset < current_offset + current_length)
                        latestOffset = current_offset + current_length;
                }
                    // need to move for compact from front
                else{
                    memcpy((char *) outputData + current_offset - length, (char *) inputData + current_offset, current_length);
                    current_offset -= length;
                    memcpy((char *) outputData + endPageOffset_out, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset_out + sizeof(int), &current_length, sizeof(int));
                    if (latestOffset < current_offset + current_length)
                        latestOffset = current_offset + current_length;
                }
            }
        }
        if(!isExist) return -1; // meaning the data doesn't exist
        // add new numberOfRecord (-1 from the old one)
        numberOfRecord -= 1;
        memcpy((char *) outputData + NUM_RECORD, &numberOfRecord, sizeof(int));
        // this will copy the first pointer in the file
        memcpy(outputData, inputData, sizeof(int));
        // put in the latest offset
        memcpy((char *) outputData + CUR_OFFSET, &latestOffset, sizeof(int));
        // put in available space
        int availableSpace;
        memcpy(&availableSpace, (char*) inputData + AVAILABLE_SPACE, sizeof(short));
        availableSpace += (length + (int)ixSLOT_SIZE);
        memcpy((char*) outputData + AVAILABLE_SPACE, &availableSpace, sizeof(short));
        memcpy(inputData, outputData, PAGE_SIZE);
        return 0;
    }
    /*
     *  helper function to split the page + insert
     */
    RC IndexManager::splitLeafPage(FileHandle &fileHandle, void* targetPage, const Attribute &attribute, int page, const void *key, const RID &rid, void* newPage, void* upperEntry){
        //we split it into 2 part: if size n then first n/2 stays and the rest goes to the new page
        int numberOfEntry;
        memcpy(&numberOfEntry, (char*) targetPage + NUM_RECORD, sizeof(int));
        int firstHalf = numberOfEntry/2;
        int secondHalf = numberOfEntry - firstHalf;
        int leftPageNum=-1;
        int rightPageNum=-1;
        //printf("i %d\n", firstHalf );
        //insert all data from second part into the new page
        for(int i = firstHalf; i < numberOfEntry; i++){
            int iSlotLocation=NEXT_PAGE - (i+1)*ixSLOT_SIZE;
            int currentOffset;
            memcpy(&currentOffset, (char*) targetPage + iSlotLocation, sizeof(int));
            int currentLength;
            memcpy(&currentLength, (char*) targetPage + iSlotLocation+sizeof(int), sizeof(int));
            //printf("OFFSET/LENGTH %d %d\n", currentOffset, currentLength);
            int keyLength=currentLength-RID_SIZE-ENTRY_POINTER;
            RID iRid;
            int pageNum;
            int slotNum;
            memcpy(&pageNum, (char*) targetPage + currentOffset + keyLength, sizeof(int));
            memcpy(&slotNum, (char*) targetPage + currentOffset + keyLength + sizeof(int), sizeof(int));
            iRid.pageNum=pageNum;
            iRid.slotNum=slotNum;
            insertNodeEntry(fileHandle, newPage, attribute, (char*) targetPage + currentOffset, iRid, rightPageNum, leftPageNum);
        }

        int dataSlotLocation=NEXT_PAGE - (firstHalf+1)*ixSLOT_SIZE;
        int currentOffset;
        memcpy(&currentOffset, (char*) targetPage +dataSlotLocation, sizeof(int));
        int currentLength;
        memcpy(&currentLength, (char*) targetPage + dataSlotLocation+sizeof(int), sizeof(int));
        int keyLength=currentLength-RID_SIZE-ENTRY_POINTER;
        char *data=new char[keyLength];
        memcpy(data, (char*) targetPage +currentOffset, keyLength);


        //delete all data of secondHalf in the old page
        for(int i = 0; i < secondHalf; i++){
            deleteNodeEntry(fileHandle, targetPage, firstHalf);
        }

        // insert key into either one of them
        if(compareKey(data, key, attribute) < 0){
            insertNodeEntry(fileHandle, targetPage, attribute, key, rid, rightPageNum, leftPageNum);
        }
        else{
            insertNodeEntry(fileHandle, newPage, attribute, key, rid, rightPageNum, leftPageNum);
        }

        memcpy((char *) newPage + NEXT_PAGE, (char*) targetPage + NEXT_PAGE, sizeof(short));
        fileHandle.appendPage(newPage);

        short newPageNum=fileHandle.numberOfPages-1;
        memcpy((char *) targetPage + NEXT_PAGE, &newPageNum, sizeof(short));
        fileHandle.writePage(page, targetPage);

        leftPageNum=page;
        rightPageNum=newPageNum;

        int newNumberOfRecord;
        memcpy(&newNumberOfRecord, (char *) newPage + NUM_RECORD, sizeof(int));
        int newEndPageOffset = (int)(NEXT_PAGE - ixSLOT_SIZE); // get the first slot
        int newStartOffset;
        memcpy(&newStartOffset, (char *) newPage + newEndPageOffset, sizeof(int));
        int newStartLength;
        memcpy(&newStartLength, (char *) newPage + newEndPageOffset + sizeof(int), sizeof(int));
        int newKeyLength=newStartLength-RID_SIZE-ENTRY_POINTER;
        //int totalLength=sizeof(int)+newKeyLength+RID_SIZE+2*ENTRY_POINTER;

        int upperOffset=sizeof(int);

        //set key length
        memcpy(upperEntry, &newKeyLength, sizeof(int));
        //set left pointer
        memcpy((char *)upperEntry + upperOffset, &leftPageNum, ENTRY_POINTER);
        upperOffset+=ENTRY_POINTER;
        //set key data
        memcpy((char *)upperEntry+upperOffset, (char *)newPage+newStartOffset, newKeyLength+RID_SIZE);
        upperOffset += newKeyLength+RID_SIZE;
        //set right pointer
        memcpy((char *)upperEntry+upperOffset, &rightPageNum, ENTRY_POINTER);

        //first root, leaf page is full. Need to create new root
        if(rootPage==page){
            char *newRootPage = new char[PAGE_SIZE];
            initializePage(newRootPage, 0);

            RID newRid;
            memcpy(&newRid.pageNum, (char*) newPage + newStartOffset + newKeyLength, sizeof(int));
            memcpy(&newRid.slotNum, (char*) newPage + newStartOffset + newKeyLength + sizeof(int), sizeof(int));
            //append new node to file
            insertNodeEntry(fileHandle, newRootPage, attribute, (char *)newPage+newStartOffset, newRid, rightPageNum, leftPageNum);

            //append new node to file
            fileHandle.appendPage(newRootPage);
            rootPage=fileHandle.numberOfPages-1;

            //save new root page id to file
            char* rootPinterData = new char[PAGE_SIZE];
            memset(rootPinterData, 0, PAGE_SIZE);
            memcpy(rootPinterData, &rootPage, sizeof(int));
            fileHandle.writePage(rootPagePointer,rootPinterData);
        }
        //printf("newChild is null? %d\n", upperEntry == nullptr);
        return 0;
    }

    RC IndexManager::splitInternalPage(FileHandle &fileHandle, void* targetPage, const Attribute &attribute, int page, const void *keyData, const RID &rid, void* newPage, void* upperEntry, int keyRightPage){
        //we split it into 2 part: if size n then first n/2 stays and the rest goes to the new page
        int numberOfEntry;
        memcpy(&numberOfEntry, (char*) targetPage + NUM_RECORD, sizeof(int));
        int firstHalf = numberOfEntry/2;
        int secondHalf = numberOfEntry - firstHalf;

        int rightPageNum;
        int newNodeLeft;

        //printf("filehandle: %d\n", fileHandle.numberOfPages);
        //insert all data from second part into the new page
        for(int i = firstHalf; i < numberOfEntry; i++){
            int iSlotLocation=NEXT_PAGE - (i+1)*ixSLOT_SIZE;
            int currentOffset;
            memcpy(&currentOffset, (char*) targetPage + iSlotLocation, sizeof(int));
            int currentLength;
            memcpy(&currentLength, (char*) targetPage + iSlotLocation+sizeof(int), sizeof(int));
            int keyLength=currentLength-RID_SIZE-ENTRY_POINTER;
            RID iRid;
            int pageNum;
            int slotNum;
            memcpy(&pageNum, (char*) targetPage + currentOffset + keyLength, sizeof(int));
            memcpy(&slotNum, (char*) targetPage + currentOffset + keyLength + sizeof(int), sizeof(int));
            iRid.pageNum=pageNum;
            iRid.slotNum=slotNum;

            memcpy(&rightPageNum, (char *)targetPage+currentOffset+currentLength-ENTRY_POINTER, ENTRY_POINTER);
            if(i==firstHalf){
                newNodeLeft=rightPageNum;
                //leftPageNum=rightPageNum;
            }

            insertNodeEntry(fileHandle, newPage, attribute, (char*) targetPage + currentOffset, iRid, rightPageNum, newNodeLeft);
        }
        //delete all data of secondHalf in the old page
        for(int i = 0; i < secondHalf; i++){
            deleteNodeEntry(fileHandle, targetPage, firstHalf);
        }

        int originalLeft;
        memcpy(&originalLeft, targetPage, ENTRY_POINTER);

        int newPageSize;
        int newPageFirstEntryOffset;
        int newPageFirstEntryLength;
        memcpy(&newPageSize, (char*)newPage + NUM_RECORD, sizeof(int));
        memcpy(&newPageFirstEntryOffset, (char*)newPage + NEXT_PAGE - ixSLOT_SIZE, sizeof(int));
        memcpy(&newPageFirstEntryLength, (char*)newPage + NEXT_PAGE - ixSLOT_SIZE + sizeof(int), sizeof(int));

        int keyLength=newPageFirstEntryLength-RID_SIZE-ENTRY_POINTER;
        char *data=new char[newPageFirstEntryLength];
        memcpy(data, (char*) newPage + newPageFirstEntryOffset, newPageFirstEntryLength);

        // insert key into either one of them
        if(compareKey(data, keyData, attribute) < 0){
            insertNodeEntry(fileHandle, targetPage, attribute, keyData, rid, keyRightPage, originalLeft);
        }
        else{
            insertNodeEntry(fileHandle, newPage, attribute, keyData, rid, keyRightPage, newNodeLeft);
        }

        //get updated first entry of second node. May be different after key insert
        memcpy(&newPageSize, (char*)newPage + NUM_RECORD, sizeof(int));
        memcpy(&newPageFirstEntryOffset, (char*)newPage + NEXT_PAGE - ixSLOT_SIZE, sizeof(int));
        memcpy(&newPageFirstEntryLength, (char*)newPage + NEXT_PAGE - ixSLOT_SIZE + sizeof(int), sizeof(int));
        keyLength=newPageFirstEntryLength-RID_SIZE-ENTRY_POINTER;
        memcpy(data, (char*) newPage + newPageFirstEntryOffset, newPageFirstEntryLength);

        int upperLeftPageNum=page;
        int upperRightPageNum=fileHandle.numberOfPages;

        int upperOffset=sizeof(int);

        //set key length
        memcpy(upperEntry, &keyLength, sizeof(int));
        //set left pointer
        memcpy((char *)upperEntry+sizeof(int), &upperLeftPageNum, ENTRY_POINTER);
        upperOffset+=ENTRY_POINTER;
        //set key data
        memcpy((char *)upperEntry+sizeof(int) +ENTRY_POINTER, (char *)data, keyLength + RID_SIZE);
        upperOffset+=keyLength+RID_SIZE;
        //set right pointer
        memcpy((char *)upperEntry+upperOffset, &upperRightPageNum, ENTRY_POINTER);

        deleteNodeEntry(fileHandle, newPage, 0);

        memcpy(&originalLeft, targetPage, ENTRY_POINTER);
        memcpy(&numberOfEntry, (char*) targetPage + NUM_RECORD, sizeof(int));
        fileHandle.writePage(page, targetPage);

        memcpy(newPage, &newNodeLeft, ENTRY_POINTER);

        fileHandle.appendPage(newPage);
        return 0;
    }

    /*
     * helper function to initialize page for node, type = 0 is root, 1 is internal node and 2 is leaf node
     */
    void IndexManager::initializePage(void* newPageData, short typeOfPage){
        int num_zero = 0;
        int off_zero = 0;
        short available_space = PAGE_SIZE - 3*sizeof(short) - 2*sizeof(int) - ENTRY_POINTER;
        short next_zero = -1;
        int entry_pointer = -1;
        memcpy((char *) newPageData + NUM_RECORD, &num_zero, sizeof(int));
        memcpy((char *) newPageData + CUR_OFFSET, &off_zero, sizeof(int));
        memcpy((char *) newPageData + TYPE_OF_NODE, &typeOfPage, sizeof(short));
        memcpy((char *) newPageData + AVAILABLE_SPACE, &available_space, sizeof(short));
        memcpy((char *) newPageData + NEXT_PAGE, &next_zero, sizeof(short));
        memcpy((char *) newPageData, &entry_pointer, sizeof(int));
    }
    /*
     * insert key into the leaf node/entry node + sorting data (rightNumPage = -1 if it is leaf node)
     * return 0 if success return -1 if fail
     */
    RC IndexManager::insertNodeEntry(FileHandle &fileHandle, void* inputData, const Attribute &attribute, const void *key, const RID &rid, int rightNumPage, int leftNumPage){
        //this function will delete that slot and move every slot after it to the right by one
        if (inputData == nullptr)
            return -1;
        //deal with the end of the file

        char* outputData = new char[PAGE_SIZE];
        int numberOfRecord;
        memcpy(&numberOfRecord, (char *) inputData + NUM_RECORD, sizeof(int));
//        if(numberOfRecord==0){
//            memcpy((char *)outputData, &leftNumPage, sizeof(int));
//        }
        memcpy((char*) outputData, (char*) inputData, ENTRY_POINTER);
        memcpy((char*) outputData + TYPE_OF_NODE, (char*) inputData + TYPE_OF_NODE, sizeof(short));
        memcpy((char*) outputData + NEXT_PAGE, (char*) inputData + NEXT_PAGE, sizeof(short));
        int endPageOffset = (int)(NEXT_PAGE - (numberOfRecord)*ixSLOT_SIZE);
        int latestOffset;
        memcpy(&latestOffset, (char*) inputData + CUR_OFFSET, sizeof(int));
        size_t length = sizeof(int) + RID_SIZE + ENTRY_POINTER;
        if (attribute.type == TypeVarChar){
            int actualLength;
            memcpy(&actualLength, key, sizeof(int));
            length += actualLength;
        }
        size_t keyLength = length - RID_SIZE - ENTRY_POINTER;
        bool isPos = false;
        //if there is no entry before it
        if(numberOfRecord == 0){
            //put left pointer
            memcpy((char*) outputData, &leftNumPage, sizeof(int));
            //copy key
            memcpy((char*) outputData + sizeof(int), key, keyLength);
            //copy rid
            memcpy((char*) outputData + sizeof(int) + keyLength, &rid.pageNum, sizeof(int));
            memcpy((char*) outputData + sizeof(int) + keyLength + sizeof(int), &rid.slotNum, sizeof(int));
            //put right pointer
            memcpy((char*) outputData + sizeof(int) + length - ENTRY_POINTER, &rightNumPage, sizeof(int));
            int current_offset = sizeof(int); // sINSERT izeof(int) is padding for leftpointer
            // put in offset
            memcpy((char*) outputData + NEXT_PAGE - ixSLOT_SIZE, &current_offset, sizeof(int));
            // put in size
            memcpy((char*) outputData + NEXT_PAGE - ixSLOT_SIZE + sizeof(int), &length, sizeof(int));
            latestOffset = length + sizeof(int);
        }
        else {
            for (int i = 0; i < numberOfRecord; i++) {
                int current_offset;
                int current_length;
                memcpy(&current_offset, (char *) inputData + endPageOffset, sizeof(int));
                memcpy(&current_length, (char *) inputData + endPageOffset + sizeof(int), sizeof(int));
                int keyLength=current_length-RID_SIZE-ENTRY_POINTER;
                //printf("i %d currentoffset %d length %d endPageOffset %d\n", i, current_offset, current_length, endPageOffset);
                char *keyData = new char[keyLength];

                memcpy(keyData, (char *) inputData + current_offset, keyLength);
                //printf("data %d key %d\n",*(int*)keyData, *(int*)key);
                int result = compareKey(keyData, key, attribute); // if result < 0 then key < data

                //if we meet the first value that is less than our key aka we need to put our new key in here before it or it needs to put as the first entry
                if (result > 0 && !isPos) {
                    //printf("data %d key %d\n",*(int*)keyData, *(int*)key);
                    isPos = true;
                    //copy key
                    memcpy((char *) outputData + latestOffset, key, keyLength);
                    //copy rid
                    memcpy((char *) outputData + latestOffset + keyLength, &rid.pageNum, sizeof(int));
                    memcpy((char *) outputData + latestOffset + keyLength + sizeof(int), &rid.slotNum, sizeof(int));
                    // insert entry pointer
                    memcpy((char *) outputData + latestOffset + length - ENTRY_POINTER, &rightNumPage, sizeof(int));
                    //latestOffset += length;
                    // put in offset
                    memcpy((char *) outputData + endPageOffset - ixSLOT_SIZE, &latestOffset, sizeof(int));
                    latestOffset += length;
                    // put in size
                    memcpy((char *) outputData + endPageOffset - ixSLOT_SIZE + sizeof(int), &length, sizeof(int));
                    //printf("put size\n");
                }
                else if(i + 1 == numberOfRecord && !isPos){
                    //printf("data %d key %d\n",*(int*)keyData, *(int*)key);
                    isPos = true;
                    //copy key
                    memcpy((char *) outputData + latestOffset, key, keyLength);
                    //copy rid
                    memcpy((char *) outputData + latestOffset + keyLength, &rid.pageNum, sizeof(int));
                    memcpy((char *) outputData + latestOffset + keyLength + sizeof(int), &rid.slotNum, sizeof(int));
                    // insert entry pointer
                    memcpy((char *) outputData + latestOffset + length - ENTRY_POINTER, &rightNumPage, sizeof(int));
                    //latestOffset += length;
                    // put in offset
                    memcpy((char *) outputData + NEXT_PAGE - ixSLOT_SIZE, &latestOffset, sizeof(int));
                    latestOffset += length;
                    // put in size
                    memcpy((char *) outputData + NEXT_PAGE - ixSLOT_SIZE + sizeof(int), &length, sizeof(int));
                }
                // in case the upper if is true then we still need to run this if for copying the data in
                if (result > 0) {
                    memcpy((char *) outputData + current_offset, (char *) inputData + current_offset, current_length);
                    memcpy((char *) outputData + endPageOffset, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset + sizeof(int), &current_length, sizeof(int));
                }
                    // need to move to the left to create space for new entry (only for slot)
                else {
                    memcpy((char *) outputData + current_offset, (char *) inputData + current_offset, current_length);
                    memcpy((char *) outputData + endPageOffset - ixSLOT_SIZE, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset + sizeof(int) - ixSLOT_SIZE, &current_length,
                           sizeof(int));
                }
                endPageOffset += (int) ixSLOT_SIZE;
            }
        }

        numberOfRecord+= 1;
        memcpy((char*)outputData + NUM_RECORD, &numberOfRecord, sizeof(int));
        // put in the latest offset
        memcpy((char *) outputData + CUR_OFFSET, &latestOffset, sizeof(int));
        // put in the update available space
        int availableSpace;
        memcpy(&availableSpace, (char*) inputData + AVAILABLE_SPACE, sizeof(short));
        availableSpace -= (length + (int)ixSLOT_SIZE);
        memcpy((char*) outputData + AVAILABLE_SPACE, &availableSpace, sizeof(short));
        memcpy(inputData, outputData, PAGE_SIZE);
        return 0;
    }

    void IndexManager::printHelper(FileHandle &fileHandle, int pageNum){
        printf("Page Num: %d\n",pageNum);
        char* targetPage = new char[PAGE_SIZE];
        fileHandle.readPage(pageNum, targetPage);
        int newnum;
        memcpy(&newnum, (char*) targetPage+NUM_RECORD, sizeof(int));
        for(int i =0;i< newnum; i++){
            int offset;
            memcpy(&offset, (char*) targetPage + NEXT_PAGE - (i+1)*ixSLOT_SIZE, sizeof(int));
            printf("%d %d KEY%d: %d OFFSET: %d PAGE: %d\n",pageNum, *(int*)((char*) targetPage), i, *(int*)((char*) targetPage + offset), offset, *(int*)((char*) targetPage + offset + 4+8));
        }
    }
    /*
     * return the page number of target page from comparing key on the page
     */
    int IndexManager::findKey(void* nodeData, const Attribute &attribute, const void *key, const RID &rid, int &slotNum){
        //get number of slot
        int numberOfSlot;
        memcpy(&numberOfSlot, (char*) nodeData + NUM_RECORD, sizeof(int));
        if(numberOfSlot == 0) return -1;// no data in this page
        int pageNum;
        for(int i = 0; i< numberOfSlot; i++){
            int dataLength;
            int dataOffset;
            memcpy(&dataOffset, (char*) nodeData + NEXT_PAGE - (i+1)*ixSLOT_SIZE, sizeof(int));
            //get the entry length, only use if this is varchar
            memcpy(&dataLength, (char*) nodeData + NEXT_PAGE - (i+1)*ixSLOT_SIZE + sizeof(int), sizeof(int));
            //if key < data then return the page
            if(compareKey((char*) nodeData + dataOffset, key, attribute) < 0){
                if(i == 0) {
                    memcpy(&pageNum, (char *) nodeData, sizeof(int));
                    memcpy(&slotNum, &i, sizeof(int));
                }
                return pageNum;
            }
            else if(i == numberOfSlot - 1){
                //if it is at the end of entry then use the most right one
                memcpy(&pageNum, (char*) nodeData + dataOffset + dataLength -ENTRY_POINTER, sizeof(int));
                memcpy(&slotNum, &i, sizeof(int));
                return pageNum;
            }
            memcpy(&pageNum, (char*) nodeData + dataOffset + dataLength - ENTRY_POINTER, sizeof(int));
            memcpy(&slotNum, &i, sizeof(int));
        }
    }

    int IndexManager::findKeyUptoLeaf(FileHandle &fileHandle, int page, const Attribute &attribute, const void *key, const RID &rid, int &slotNum){

        char *targetPage = new char[PAGE_SIZE];
        if(fileHandle.readPage(page, targetPage) != 0)
            return -1;
        short isLeaf;
        memcpy(&isLeaf, (char*)targetPage + TYPE_OF_NODE, sizeof(short));
        if (isLeaf == 0 || isLeaf == 1) {
            //printf("isLeaf %d\n", isLeaf);
            int newPageNum;
            if(key== nullptr){
                memcpy(&newPageNum, (char *)targetPage, ENTRY_POINTER);
            }
            else{
                newPageNum = findKey(targetPage, attribute, key, rid, slotNum);
            }
            //printf("newPagenum %d\n", newPageNum);
            return findKeyUptoLeaf(fileHandle, newPageNum, attribute, key, rid, slotNum);
        }
        else if(isLeaf==2){
            if(key== nullptr){
                slotNum=0;
            } else{
                findKey(targetPage, attribute, key, rid, slotNum);
            }
            return page;
        }
        return -1;
    }
    /*
     *  comparing function
     *  return 0 if equal, return 1 if data is less than key and return -1 if data is more than key
     */
    int IndexManager::compareKey(const void* data, const void* key, const Attribute &attribute){
        if(attribute.type == TypeVarChar){
            int dataLength;
            int keyLength;
            memcpy(&dataLength, data, sizeof(int));
            memcpy(&keyLength, key, sizeof(int));
            int d_page;
            int d_slot;
            int k_page;
            int k_slot;
            memcpy(&d_page, (char*)data + sizeof(int) + dataLength, sizeof(int));
            memcpy(&k_page, (char*)key + sizeof(int) + dataLength, sizeof(int));
            memcpy(&d_slot, (char*)data + 2*sizeof(int) +dataLength, sizeof(int));
            memcpy(&k_slot, (char*)key + 2*sizeof(int) + dataLength, sizeof(int));
            string d((char *)data + sizeof(int), dataLength);
            string k((char *)key + sizeof(int), keyLength);
            int result = k.compare(d);
            if(result == 0) {
//                if(d_page > k_page || (d_page == k_page && d_slot > k_slot))
//                    return -1;
//                else if(d_page < k_page || (d_page == k_page && d_slot < k_slot))
//                    return 1;
//                else
//                    return 0;
                return 0;
            }
            else if(result < 0) return -1;
            else
                return 1;
        }
        else if(attribute.type == TypeInt){
            int d;
            int k;
            //printf("d=%d, k=%d\n", d, k);
            memcpy(&d, data, sizeof(int));
            memcpy(&k, key, sizeof(int));
            int d_page;
            int d_slot;
            int k_page;
            int k_slot;
            memcpy(&d_page, (char*)data + sizeof(int), sizeof(int));
            memcpy(&k_page, (char*)key + sizeof(int), sizeof(int));
            memcpy(&d_slot, (char*)data + 2*sizeof(int), sizeof(int));
            memcpy(&k_slot, (char*)key + 2*sizeof(int), sizeof(int));
            if(d == k){
//                if(d_page > k_page || (d_page == k_page && d_slot > k_slot))
//                    return -1;
//                else if(d_page < k_page || (d_page == k_page && d_slot < k_slot))
//                    return 1;
//                else
//                    return 0;
                return 0;
            }
            else if(d > k)
                return -1;
            else
                return 1;
        }
        else{
            //for TypeReal
            float d;
            float k;
            memcpy(&d, data, sizeof(float));
            memcpy(&k, key, sizeof(float));
            int d_page;
            int d_slot;
            int k_page;
            int k_slot;
            memcpy(&d_page, (char*)data + sizeof(int), sizeof(int));
            memcpy(&k_page, (char*)key + sizeof(int), sizeof(int));
            memcpy(&d_slot, (char*)data + 2*sizeof(int), sizeof(int));
            memcpy(&k_slot, (char*)key + 2*sizeof(int), sizeof(int));
            if(d == k){
//                if(d_page > k_page || (d_page == k_page && d_slot > k_slot))
//                    return -1;
//                else if(d_page < k_page || (d_page == k_page && d_slot < k_slot))
//                    return 1;
//                else
//                    return 0;
                return 0;
            }
            else if(d > k)
                return -1;
            else
                return 1;
        }
    }

    int IndexManager::deleteKey(FileHandle &fileHandle, const Attribute &attribute, int page, const void *key, const RID &rid, void* oldChildEntry){
        //check if this node is leaf node or none-leaf node
        char *targetPage = new char[PAGE_SIZE];
        if(page == -1)
            return -1;
        if(fileHandle.readPage(page, targetPage) != 0)
            return -1;
        short isLeaf;
        //  printf("deleteKey\n");
        memcpy(&isLeaf, (char*)targetPage + TYPE_OF_NODE, sizeof(short));
        int slotNum;
        // printf("isLeaf %d %d\n", isLeaf, page);
        //if this is an non-leaf node (0 for root, 1 for internal node)
        if (isLeaf == 0 || isLeaf == 1){
            int newPageNum = findKey(targetPage, attribute, key, rid, slotNum);
            int result = deleteKey(fileHandle, attribute, newPageNum, key, rid, oldChildEntry);
            return result;
        }
            //if this is a leaf node
        else if (isLeaf == 2){
            int numberOfEntry;
            memcpy(&numberOfEntry, (char*) targetPage + NUM_RECORD, sizeof(int));
            findKey(targetPage, attribute, key, rid, slotNum);
            if(deleteNodeEntry(fileHandle, targetPage, slotNum) == -1)
                return -1;
            oldChildEntry = nullptr;
            fileHandle.writePage(page, targetPage);
            return 0;
        }
        else
            return -1;
    }
} // namespace PeterDB