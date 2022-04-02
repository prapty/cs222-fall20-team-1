#include <iostream>
#include <string>
#include "src/include/pfm.h"
#include <sys/stat.h>
#include <cstring>
#include <regex>
#include <cmath>
#include <src/include/rbfm.h>

using namespace std;

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;
    /*
     * Create a file with given name fileName
     * return 0 if success
     * return -1 otherwise
     */
    RC PagedFileManager::createFile(const std::string &fileName) {
        // check if fileName already exists or not
        struct stat stat_buffer;
        if(stat(fileName.c_str(), &stat_buffer) == 0) {
            //fileName already exists return -1
            return -1;
        }
        else
        {
            //fileName doesn't exist -> create fileName -> return 0
            FILE *f = fopen(fileName.c_str(), "wb");

            int offset = 0;
            char *data = new char[PAGE_SIZE];
            memset(data, 0, PAGE_SIZE);
            //append hidden first page with initial counter values and number of pages
            unsigned readPageCounter=0;
            unsigned writePageCounter=0;
            //increase appendPageCounter for appending hidden page
            unsigned appendPageCounter =1;
            unsigned numberOfPages =0;
            unsigned numberOfHiddenPages=1;

            memcpy((char *) data+offset, &readPageCounter, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &writePageCounter, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &appendPageCounter, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &numberOfPages, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &numberOfHiddenPages, sizeof(unsigned));

            fwrite(data,1,PAGE_SIZE,f);
            fclose(f);
            return 0;
        }
    }
    /*
     * destroy a file with given name fileName
     * return 0 if success
     * return -1 otherwise
     */
    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(remove(fileName.c_str()) == 0)
            return 0;
        else
            return -1;
    }
    /*
     * open a file with given name fileName
     * return 0 if success
     * return -1 otherwise
     */
    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        //FILE *f = fopen(fileName.c_str(), "r+");
        FILE *f = fopen(fileName.c_str(), "rb+");
        // if fopen cannot find the file name fileName return -1
        if(!f)
            return -1;
            // else give the FILE pointer to the fileName to fileHandle and return 0
        else {
            char *data = new char[PAGE_SIZE];
            memset(data, 0, PAGE_SIZE);
            fread (data,1,PAGE_SIZE,f);

            int offset =0;

            memcpy(&fileHandle.readPageCounter, (char *) data+offset, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy(&fileHandle.writePageCounter, (char *) data+offset, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy(&fileHandle.appendPageCounter, (char *) data+offset, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy(&fileHandle.numberOfPages, (char *) data+offset, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy(&fileHandle.numberOfHiddenPages, (char *) data+offset, sizeof(unsigned));
            offset+=sizeof(unsigned);

            fileHandle.pfmDataEndPos=offset;

            fileHandle.readPageCounter+=1;
            fileHandle.F_pointer = f;
            fileHandle.fileName = fileName;
            return 0;
        }
    }

    /*
     * close a file with given name fileName
     * return 0 if success
     * return -1 otherwise
     */
    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if(fileHandle.F_pointer == NULL){
            return -1;
        }
            //  close the file and reset this fileHandle
            //  not sure about this => "All of the file's pages are flushed to disk when the file is closed."
            //it means that in OS some data may have been in buffer instead of being in disk. While executing fclose(), those data will be written in disk
        else{
            fileHandle.writePageCounter+=1;

            char *data = new char[PAGE_SIZE];
            memset(data, 0, PAGE_SIZE);
            rewind(fileHandle.F_pointer);
            fread (data,1,PAGE_SIZE,fileHandle.F_pointer);

            int offset =0;

            memcpy((char *) data+offset, &fileHandle.readPageCounter, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &fileHandle.writePageCounter, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &fileHandle.appendPageCounter, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &fileHandle.numberOfPages, sizeof(unsigned));
            offset+=sizeof(unsigned);

            memcpy((char *) data+offset, &fileHandle.numberOfHiddenPages, sizeof(unsigned));

            rewind(fileHandle.F_pointer);
            fwrite(data,1,PAGE_SIZE,fileHandle.F_pointer);

            /*int secondPartSize = PAGE_SIZE-fileHandle.pfmDataEndPos;
            char debug[secondPartSize];
            fseek(fileHandle.F_pointer,fileHandle.pfmDataEndPos,SEEK_SET);
            fread(debug, 1, secondPartSize, fileHandle.F_pointer);
            string debugStr(debug, secondPartSize);*/

            fclose(fileHandle.F_pointer);

            fileHandle.readPageCounter = 0;
            fileHandle.writePageCounter = 0;
            fileHandle.appendPageCounter = 0;
            fileHandle.numberOfPages=0;
            fileHandle.F_pointer = NULL;
            return 0;
        }
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        numberOfPages = 0;
        //File pointer
        F_pointer = NULL;
        fileName = " ";
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        //check if file is open
        if(F_pointer == NULL){
            return -1;
        }
        else{
            //get file size
            fseek (F_pointer , 0 , SEEK_END);
            long fileSize = ftell (F_pointer);
            rewind (F_pointer);

            unsigned int previousHiddenPages = ((unsigned int)floor(pageNum/hiddenPageRate))+1;
            //first page is hidden to store counter values
            //int actualPageNum=pageNum+numberOfHiddenPages;
            unsigned int actualPageNum=pageNum+previousHiddenPages;

            unsigned int location= actualPageNum*PAGE_SIZE;
            //printf("location in read page: %d\n",location);
            //check whether given page exists
            if(location>fileSize){
                return -1;
            }
            else{
                //reposition stream position indicator to required page
                fseek(F_pointer,location,SEEK_SET);
                //read page
                fread (data,1,PAGE_SIZE,F_pointer);
                readPageCounter = readPageCounter+1;
                return 0;
            }
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        //check if file is open
        if(F_pointer == NULL){
            return -1;
        }
        else{
            //get file size
            //rewind (F_pointer);
            fseek (F_pointer , 0 , SEEK_END);
            long fileSize = ftell (F_pointer);
            rewind (F_pointer);

            unsigned previousHiddenPages = (pageNum/hiddenPageRate)+1;

            //first page is hidden to store counter values

            int actualPageNum=pageNum+previousHiddenPages;
            //int actualPageNum=(int)pageNum+numberOfHiddenPages;

            int location= actualPageNum*PAGE_SIZE;
            //printf("location in write page: %d\n",location);
            //check whether given page exists
            if(location>fileSize){
                return -1;
            }
            else{
                //reposition stream position indicator to required page
                fseek(F_pointer,location,SEEK_SET);
                //write page
                fwrite(data,1,PAGE_SIZE,F_pointer);
                writePageCounter = writePageCounter+1;
                //fflush(F_pointer);
                return 0;
            }
        }
    }

    RC FileHandle::appendPage(const void *data) {
        //check if file is open
        //printf("Call to append\n");
        if(F_pointer == NULL){
            return -1;
        } else{
            int numberOfRecord;
            void *endOfPage = (char *) data + NUM_RECORD; // get to data about number of records
            memcpy(&numberOfRecord, (char *) endOfPage, sizeof(int));
            //printf("numRecord in appendPage %d\n", numberOfRecord);
            fseek(F_pointer,0,SEEK_END);
            long fileSize = ftell (F_pointer);
            //printf("File size before append:%d\n",fileSize);
            fwrite(data,1,PAGE_SIZE,F_pointer);
            fseek(F_pointer,0,SEEK_END);
            fileSize = ftell (F_pointer);
            //printf("File size after append:%d\n",fileSize);
            appendPageCounter = appendPageCounter+1;
            numberOfPages = numberOfPages+1;
            if(numberOfPages%hiddenPageRate==0){
                char *hidden=new char[PAGE_SIZE];
                memset(hidden, 0, PAGE_SIZE);
                fwrite(hidden,1,PAGE_SIZE,F_pointer);
                fseek(F_pointer,0,SEEK_END);
                fileSize = ftell (F_pointer);
                //printf("File size after extra hidden page:%d\n",fileSize);
                //free(hidden);
                //fflush(F_pointer);
            }
            return 0;
        }
    }

    unsigned FileHandle::getNumberOfPages() {
        return numberOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

} // namespace PeterDB