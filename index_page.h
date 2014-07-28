#ifndef EXTENDIBLE_HASHING_DB_INDEXPAGE_H_
#define EXTENDIBLE_HASHING_DB_INDEXPAGE_H_

#include "global.h"

extern const int buffer_size;
extern const int page_size;


const int indexBuf_size = 2;
/* index_num = page_size / sizeof(long) - 1 */
const int index_num = page_size / sizeof(long) - 1;

struct indexPage
{
  long indexPageId;
  long index[index_num];
};

class IndexPage
{
  public:
    IndexPage();
    ~IndexPage();
    void save();
    void createIndex();
    void readIndex();
    long getGlobalDepth();
    long getBucketId(const long dir_num);
    int loadAPage(const long pageId);
    int checkPage(const long pageId);
    int requestAPage();
    int clockReplace();
    void writeBufPage(const int buf_id);
    void readBufPage(const int buf_id, const int indexPageId);
    void doubling();
    int pinAPage(const int indexPageId);
    void relocation(const long initDir, const long bucketId, const int initDepth);
    void setCmd(char *ptr);

  private:
    char cmd[100];
    char globalDepthFile[100];
    long globalDepth;
    int current;
    int pageNum;
    int pin_count[indexBuf_size];
    bool ref_bit[indexBuf_size];
    bool dirty[indexBuf_size];
    indexPage indexBuf[indexBuf_size];
};

#endif
