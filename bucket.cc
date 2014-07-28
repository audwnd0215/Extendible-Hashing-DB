#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include "bucket.h"

extern long IOcount;
extern long IOtime;

Bucket::Bucket()
{
  cmd[0] = '\0';
  for (int i = 0; i < bucketBuf_size; i++)
  {
    pin_count[i] = 0;
    ref_bit[i] = false;
    dirty[i] = false;
    bucketBuf[i].slotNum = 0;
    bucketBuf[i].toFreeSpace = 0;
    bucketBuf[i].localDepth = 1;
    bucketBuf[i].bucketId = -1;
  }
  current = 0;
  ref_bit[0] = ref_bit[1] = true;
  dirty[0] = dirty[1] = true;
  bucketBuf[0].bucketId = 0;
  bucketBuf[1].bucketId = 1;
  pageNum = 2;
}

Bucket::~Bucket()
{
  save();
}

void Bucket::save()
{
  for (int i = 0; i < bucketBuf_size; i++)
  {
    if (dirty[i])
      writeBufPage(i);
  }
}

int Bucket::checkPage(const long pageId)
{
  int i;

  for (i = 0; i < bucketBuf_size; i++)
  {
    // page id must match
    if (bucketBuf[i].bucketId == pageId)
      return i;
  }
  return -1;
}

int Bucket::requestAPage()
{
  int repBuf;

  repBuf = clockReplace();
  if (dirty[repBuf])
    writeBufPage(repBuf);

  pin_count[repBuf] = 0;
  ref_bit[repBuf] = false;
  dirty[repBuf] = false;

  return repBuf;
}

int Bucket::clockReplace()
{
  /* flag = true when the clock replacement algorithm has been
     implemented one time */
  bool flag = false;
  int tmp = current;

  while (1)
  {
    if (pin_count[current] == 0)
    {
      if (ref_bit[current] == true)
        ref_bit[current] = false;
      else
        return current;
    }
    current = (current + 1) % bucketBuf_size;
    if (tmp == current)
    {
      if (flag)
        return current;
      flag = true;
    }
  }
}

void Bucket::writeBufPage(const int buf_id)
{
  FILE *pFile;
  clock_t tmpStart, tmpEnd;

  tmpStart = clock();
  pFile = fopen(cmd, "rb+");
  if (pFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }

  fseek(pFile, bucketBuf[buf_id].bucketId * page_size, SEEK_SET);
  fwrite(&bucketBuf[buf_id], 1, page_size, pFile);
  fclose(pFile);
  tmpEnd = clock();
  IOtime += (tmpEnd - tmpStart);
  IOcount++;
}

void Bucket::readBufPage(const int buf_id, const long pageId)
{
  FILE *pFile;
  clock_t tmpStart, tmpEnd;

  if ((pFile = fopen(cmd, "rb")) == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  tmpStart = clock();
  fseek(pFile, pageId * page_size, SEEK_SET);
  fread(&bucketBuf[buf_id], 1, page_size, pFile);
  fclose(pFile);
  tmpEnd = clock();
  IOtime += (tmpEnd - tmpStart);
  IOcount++;
  ref_bit[buf_id] = true;
}

int Bucket::pinAPage(const int pageId)
{
  int buf;

  buf = checkPage(pageId);
  if (buf == -1)
  {
    buf = requestAPage();
    readBufPage(buf, pageId);
  }
  pin_count[buf]++;
  ref_bit[buf] = true;

  return buf;
}

int Bucket::insert(char *ptr, const int start, const int end, const long pageId, long & splitBucketId, long & localDepth)
{
  int cas, buf_id, length = end - start;

  localDepth = 0;
  buf_id = pinAPage(pageId);
  if (bucketBuf[buf_id].slotNum < record_num && length + bucketBuf[buf_id].toFreeSpace < char_num)
  {
    long offset = bucketBuf[buf_id].toFreeSpace;

    memcpy(bucketBuf[buf_id].data + offset, ptr + start, length);
    bucketBuf[buf_id].slotDir[bucketBuf[buf_id].slotNum].offset = offset;
    bucketBuf[buf_id].slotDir[bucketBuf[buf_id].slotNum].length = length;
    bucketBuf[buf_id].slotNum++;
    bucketBuf[buf_id].toFreeSpace += length;
    dirty[buf_id] = true;
    cas = 0;	// Case 0#: insert into a bucket without split
  }
  else
  {
    // cause bucket splitting
    localDepth = ++bucketBuf[buf_id].localDepth;
    dirty[buf_id] = true;
    cas = split(buf_id, splitBucketId);
    if (cas == 2)	// Case #2. Split success! We must modify the splited bucket's information
      modifyInfo(buf_id);
  }

  pin_count[buf_id]--;

  return cas;
}

int Bucket::split(const int buf_id, long & bucketId)
{
  int buf, i, k, cas;
  long key, srcOffset, dstOffset, length, depth = bucketBuf[buf_id].localDepth;
  bool splitBit[record_num];

  buf = requestAPage();
  pin_count[buf]++;
  ref_bit[buf] = true;
  dirty[buf] = true;
  bucketBuf[buf].slotNum = bucketBuf[buf].toFreeSpace = 0;
  bucketBuf[buf].localDepth = depth;
  bucketBuf[buf].bucketId = pageNum++;	// this is a new page
  bucketId = bucketBuf[buf].bucketId;

  // get all splitBit
  for (i = 0; i < bucketBuf[buf_id].slotNum; i++)
  {
    srcOffset = bucketBuf[buf_id].slotDir[i].offset;
    for(k = srcOffset; bucketBuf[buf_id].data[k] != '|'; k++)
      ;
    key = toLong(bucketBuf[buf_id].data, srcOffset, k);
    splitBit[i] = getSplitBit(key, depth);
    if (splitBit[i])
      bucketBuf[buf].slotNum++;
  }

  if (bucketBuf[buf].slotNum == bucketBuf[buf_id].slotNum)
  {
    // Case 1#: split fail. The records of initial buffer will be cleared.
    bucketBuf[buf] = bucketBuf[buf_id];
    bucketBuf[buf].bucketId = bucketId;
    bucketBuf[buf_id].slotNum = bucketBuf[buf_id].toFreeSpace = 0;	// clear the initial buffer
    cas = 1;
  }
  else if (bucketBuf[buf].slotNum != 0)	// Case 2#: split success! We get two nonempty buffers.
  {
    bucketBuf[buf].slotNum = 0;
    for (i = 0; i < bucketBuf[buf_id].slotNum; i++)
    {
      if (splitBit[i])
      {
        srcOffset = bucketBuf[buf_id].slotDir[i].offset;
        length = bucketBuf[buf_id].slotDir[i].length;
        dstOffset = bucketBuf[buf].toFreeSpace;
        memcpy(bucketBuf[buf].data + dstOffset, bucketBuf[buf_id].data + srcOffset, length);
        bucketBuf[buf].slotDir[bucketBuf[buf].slotNum].offset = dstOffset;
        bucketBuf[buf].slotDir[bucketBuf[buf].slotNum].length = length;
        bucketBuf[buf].slotNum++;
        bucketBuf[buf].toFreeSpace += length;

        // mark it so that we can modify the splited bucket's information later
        bucketBuf[buf_id].slotDir[i].length = -1;
      }
    }
    cas = 2;
  }
  else	//  Case 3#: split fail. The split buffer is empty.
    cas = 3;

  pin_count[buf]--;

  return cas;
}


void Bucket::modifyInfo(const int buf_id)
{
  int i;
  long toFreeSpace, length, slotNum = 0;
  bool flag = false;

  pin_count[buf_id]++;
  for(i = 0; i < bucketBuf[buf_id].slotNum; i++)
  {
    if (!flag && bucketBuf[buf_id].slotDir[i].length == -1)
    {
      flag = true;
      toFreeSpace = bucketBuf[buf_id].slotDir[i].offset;
      continue;
    }
    else if (flag)
    {
      length = bucketBuf[buf_id].slotDir[i].length;
      if (length == -1)
        continue;

      memcpy(bucketBuf[buf_id].data + toFreeSpace, 
          bucketBuf[buf_id].data + bucketBuf[buf_id].slotDir[i].offset, length);
      bucketBuf[buf_id].slotDir[slotNum].offset = toFreeSpace;
      bucketBuf[buf_id].slotDir[slotNum].length = length;
      toFreeSpace += length;
    }
    slotNum++;
  }

  bucketBuf[buf_id].slotNum = slotNum;
  bucketBuf[buf_id].toFreeSpace = toFreeSpace;
  pin_count[buf_id]--;
}

long Bucket::getLocalDepth(const long pageId)
{
  int buf_id = pinAPage(pageId);

  pin_count[buf_id]--;
  return bucketBuf[buf_id].localDepth;
}

void Bucket::setCmd(char *ptr)
{
  strcpy(cmd, ptr);
  strcat(cmd, "/LineItemRecord.out");
}

void Bucket::createBucketFile()
{
  FILE *pFile;

  pFile = fopen(cmd, "wb");	// create a file
  if (pFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  fclose(pFile);
}

int comp(const void *a, const void *b)
{
  return ((HitRecord*)a)->partkey - ((HitRecord*)b)->partkey;
}

void Bucket::query(const long pageId, const long orderkey, const char *outfile)
{
  int buf_id = pinAPage(pageId);
  long i, cur, k, offset, length, hitNum = 0;
  HitRecord hitRecord[record_num];
  FILE *pFile;
  clock_t tmpStart, tmpEnd;

  for (i = 0; i < bucketBuf[buf_id].slotNum; i++)
  {
    cur = bucketBuf[buf_id].slotDir[i].offset;
    for (k = cur; bucketBuf[buf_id].data[k] != '|'; k++)
      ;

    if (orderkey == toLong(bucketBuf[buf_id].data, cur, k))	// compare orderkey
    {
      for (k++, cur = k; bucketBuf[buf_id].data[k] != '|'; k++)
        ;
      hitRecord[hitNum].partkey = toLong(bucketBuf[buf_id].data, cur, k);
      hitRecord[hitNum].slotId = i;
      hitNum++;
    }
  }

  qsort(hitRecord, hitNum, sizeof(HitRecord), comp);

  tmpStart = clock();
  pFile = fopen(outfile, "ab");
  if (pFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }

  for (i = 0; i < hitNum; i++)
  {
    k = hitRecord[i].slotId;
    offset = bucketBuf[buf_id].slotDir[k].offset;
    length = bucketBuf[buf_id].slotDir[k].length;
    fwrite(&bucketBuf[buf_id].data[offset], sizeof(char), length, pFile);
  }
  fclose(pFile);
  tmpEnd = clock();
  IOtime += (tmpEnd - tmpStart);
  IOcount++;
  pin_count[buf_id]--;
}

void Bucket::readBucket()
{
  FILE *pFile;
  clock_t tmpStart, tmpEnd;

  if ((pFile = fopen(cmd, "rb")) == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  else
  {
    tmpStart = clock();
    fread(&bucketBuf[0], 1, page_size, pFile);
    fread(&bucketBuf[1], 1, page_size, pFile);
    fclose(pFile);
    tmpEnd = clock();
    IOtime += (tmpEnd - tmpStart);
    IOcount++;
    ref_bit[0] = true;
    ref_bit[1] = true;
  }
}
