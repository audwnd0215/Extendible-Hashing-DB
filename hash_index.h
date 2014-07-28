#ifndef EXTENDIBLE_HASHING_DB_HASHINDEX_H_
#define EXTENDIBLE_HASHING_DB_HASHINDEX_H_

#include <ctime>
#include "index_page.h"
#include "bucket.h"
#include "global.h"

extern const int page_size;

class HashIndex
{
  public:
    HashIndex();
    ~HashIndex();
    long toLong(char *ptr, int start, int end);
    void createIndex();
    int readADataPage(const long offsetOfRead);
    long MSHash(const long key);
    void setCmd(char *ptr);
    void query();

  private:
    char cmd[100];
    char queryInFile[100];
    char queryOutFile[100];
    char dataBuf[page_size];
    IndexPage indexMgr;
    Bucket bucketMgr;
    clock_t startTime, endTime;
};

inline long HashIndex::toLong(char *ptr, int start, int end)
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

inline long HashIndex::MSHash(const long key)
{
  const long mask = 1;
  long ans = 0, globalDepth = indexMgr.getGlobalDepth();
  int i, bitCount = 1;

  while (key >> bitCount)	// calculate that how many bits the key have in binary
    bitCount++;

  ans |= (mask << (bitCount - 1));
  for (i = 0; i < bitCount - 1; i++)
    ans |= (((key >> i) & mask) << (bitCount - 2 - i));

  if (bitCount > globalDepth)
    ans &= ((mask << globalDepth) - 1);
  // if bitCount < globalDepth, we will add zeros in the most significant bits

  return ans;
}

#endif
