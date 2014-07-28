#ifndef EXTENDIBLE_HASHING_DB_BUCKET_H_
#define EXTENDIBLE_HASHING_DB_BUCKET_H_

#include "global.h"

extern const int buffer_size;
extern const int page_size;

const int bucketBuf_size = 5;
/* Through a simple test we know that each record takes average 120 
   Bytes approximately. Since 8192 / 120 = 68, we assume that the maximum 
   number of records in a page is 70. The remain memory space for record 
   data is 8192 - sizeof(long)*4 - sizeof(slotDir) = 7616. */
const int record_num = 70;
const int char_num = 7616;

struct RecordInfo
{
  long offset;
  long length;
};

struct bucketPage
{
  char data[char_num];
  RecordInfo slotDir[record_num];
  long slotNum;
  long toFreeSpace;
  long localDepth;
  long bucketId;
};

struct HitRecord
{
  long slotId;
  long partkey;
};

class Bucket
{
  public:
    Bucket();
    ~Bucket();
    void save();
    long toLong(char *ptr, int start, int end);
    int checkPage(const long pageId);
    int requestAPage();
    int clockReplace();
    void writeBufPage(const int buf_id);
    void readBufPage(const int buf_id, const long pageId);
    int pinAPage(const int pageId);
    int insert(char *ptr, const int start, const int end, const long pageId, long & splitBucketId, long & localDepth);
    int split(const int buf_id, long & bucketId);
    bool getSplitBit(const long key, const long depth);
    void modifyInfo(const int buf_id);
    long getLocalDepth(const long pageId);
    void setCmd(char *ptr);
    void createBucketFile();
    void query(const long pageId, const long orderkey, const char *outfile);
    void readBucket();

  private:
    char cmd[100];
    int current;
    int pageNum;
    int pin_count[bucketBuf_size];
    bool ref_bit[bucketBuf_size];
    bool dirty[bucketBuf_size];
    bucketPage bucketBuf[bucketBuf_size];
};


inline long Bucket::toLong(char *ptr, int start, int end)
{
  long res = 0;

  if (start >= end)
    return -1;

  do {
    res = res * 10 + ptr[start]- '0';
    start++;
  } while (start < end);

  return res;
}


inline bool Bucket::getSplitBit(const long key, const long depth)
{
  const long mask = 1;
  long ans = 0;
  int i, bitCount = 1;

  while (key >> bitCount)	// calculate that how many bits the key have in binary
    bitCount++;

  if (bitCount < depth)
    return false;
  else
  {
    ans |= (mask << (bitCount - 1));
    for (i = 0; i < bitCount - 1; i++)
      ans |= (((key >> i) & mask) << (bitCount - 2 - i));

    return (ans >> (depth - 1)) & 1;
  }
}

#endif
