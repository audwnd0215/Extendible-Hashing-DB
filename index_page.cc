#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include "index_page.h"

extern long IOcount;
extern long IOtime;

IndexPage::IndexPage()
{
  cmd[0] = '\0';
  for (int i = 0; i < indexBuf_size; i++)
  {
    pin_count[i] = 0;
    ref_bit[i] = false;
    dirty[i] = false;
  }
  current = 0;
  pageNum = 0;
}

IndexPage::~IndexPage()
{
  save();
}

void IndexPage::save()
{
  FILE *pFile;
  clock_t tmpStart, tmpEnd;

  for (int i = 0; i < indexBuf_size; i++)
  {
    if (dirty[i])
      writeBufPage(i);
  }

  tmpStart = clock();
  pFile = fopen(globalDepthFile, "wb");
  if (pFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }

  fwrite(&globalDepth, 1, sizeof(globalDepth), pFile);
  fclose(pFile);
  tmpEnd = clock();
  IOtime += (tmpEnd - tmpStart);
  IOcount++;
}

void IndexPage::createIndex()
{
  FILE *pFile;

  pFile = fopen(cmd, "wb");	// create a file
  if (pFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  fclose(pFile);

  pFile = fopen(globalDepthFile, "wb");
  if (pFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  fclose(pFile);

  globalDepth = 1;
  ref_bit[0] = true;
  dirty[0] = true;
  indexBuf[0].indexPageId = pageNum++;
  indexBuf[0].index[0] = 0;
  indexBuf[0].index[1] = 1;
}

void IndexPage::readIndex()
{
  FILE *pFile, *tFile;
  clock_t tmpStart, tmpEnd;

  if ((pFile = fopen(cmd, "rb")) == NULL || (tFile = fopen(globalDepthFile, "rb")) == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  else
  {
    tmpStart = clock();
    fread(&indexBuf[0], 1, page_size, pFile);
    fclose(pFile);
    tmpEnd = clock();
    IOtime += (tmpEnd - tmpStart);
    IOcount++;
    ref_bit[0] = true;

    tmpStart = clock();
    fread(&globalDepth, 1, sizeof(globalDepth), tFile);
    fclose(tFile);
    tmpEnd = clock();
    IOtime += (tmpEnd - tmpStart);
    IOcount++;
  }
}

long IndexPage::getGlobalDepth()
{
  return globalDepth;
}

long IndexPage::getBucketId(const long dir_num)
{
  int buf, indexPageId = dir_num / index_num;

  buf = loadAPage(indexPageId);

  return indexBuf[buf].index[dir_num % index_num];
}

int IndexPage::loadAPage(const long pageId)
{
  int buf = checkPage(pageId);

  if (buf == -1)
  {
    buf = requestAPage();
    readBufPage(buf, pageId);
  }

  return buf;
}

int IndexPage::checkPage(const long pageId)
{
  int i;

  for (i = 0; i < indexBuf_size; i++)
  {
    // page id must match
    if (indexBuf[i].indexPageId == pageId)
    {
      ref_bit[i] = true;
      return i;
    }
  }
  return -1;
}

int IndexPage::requestAPage()
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

int IndexPage::clockReplace()
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
    current = (current + 1) % indexBuf_size;
    if (tmp == current)
    {
      if (flag)
        return current;
      flag = true;
    }
  }
}

void IndexPage::writeBufPage(const int buf_id)
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

  fseek(pFile, indexBuf[buf_id].indexPageId * page_size, SEEK_SET);
  fwrite(&indexBuf[buf_id], 1, page_size, pFile);
  fclose(pFile);
  tmpEnd = clock();
  IOtime += (tmpEnd - tmpStart);
  IOcount++;
}

void IndexPage::readBufPage(const int buf_id, const int indexPageId)
{
  FILE *pFile;
  clock_t tmpStart, tmpEnd;

  if ((pFile = fopen(cmd, "rb")) == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  tmpStart = clock();
  fseek(pFile, indexPageId * page_size, SEEK_SET);
  fread(&indexBuf[buf_id], 1, page_size, pFile);
  fclose(pFile);
  tmpEnd = clock();
  IOtime += (tmpEnd - tmpStart);
  IOcount++;
  ref_bit[buf_id] = true;
}

void IndexPage::doubling()
{
  int src_buf, dst_buf;
  long src_dir, dst_dir, src_PageId, dst_PageId, total = 1 << globalDepth;

  src_dir = 0;
  src_PageId = 0;
  src_buf = pinAPage(src_PageId);

  dst_dir = total;
  dst_PageId = dst_dir / index_num;
  if (dst_PageId != src_PageId)
    dst_buf = pinAPage(dst_PageId);
  else
  {
    dst_buf = src_buf;
    pin_count[dst_buf]++;
    ref_bit[dst_buf] = true;
  }
  dirty[dst_buf] = true;		// dst_buf need to be written to disk

  int i = 0, k = dst_dir % index_num;
  while (1)
  {
    while (src_dir < total && i < index_num && k < index_num)
    {
      indexBuf[dst_buf].index[k] = indexBuf[src_buf].index[i]; 
      src_dir++;
      dst_dir++;
      i++;
      k++;
    }

    if (src_dir >= total)
    {
      pin_count[dst_buf]--;
      ref_bit[dst_buf] = false;
      pin_count[src_buf]--;
      ref_bit[src_buf] = false;
      break;
    }

    if (i >= index_num)
    {
      pin_count[src_buf]--;
      ref_bit[src_buf] = false;
      src_PageId++;
      if (src_PageId != dst_PageId)
        src_buf = pinAPage(src_PageId);
      else
      {
        src_buf = dst_buf;
        pin_count[src_buf]++;
      }
      i = 0;
    }

    if (k >= index_num)
    {
      pin_count[dst_buf]--;
      ref_bit[dst_buf] = false;
      dst_PageId++;
      // Since (dst_PageId + 1) is greater than src_PageId, thus they are inequal.
      dst_buf = pinAPage(dst_PageId);
      dirty[dst_buf] = true;		// dst_buf need to be written to disk
      k = 0;
    }
  }
  globalDepth++;
}

int IndexPage::pinAPage(const int indexPageId)
{
  int buf;

  if (indexPageId < pageNum)
    buf = loadAPage(indexPageId);
  else
  {
    buf = requestAPage();
    indexBuf[buf].indexPageId = pageNum++;
  }
  pin_count[buf]++;
  ref_bit[buf] = true;

  return buf;
}


void IndexPage::relocation(const long initDir, const long bucketId, const int initDepth)
{
  int buf;
  long total, indexPageId, i, dir_num, step = 1  << initDepth, jump = 1  << (globalDepth - 1);

  total = jump / step;
  dir_num = initDir % step + step;
  step <<= 1;
  for (i = 0; i < total; i++, dir_num += step)
  {
    indexPageId = dir_num / index_num;
    buf = loadAPage(indexPageId);
    indexBuf[buf].index[dir_num % index_num] = bucketId;
    dirty[buf] = true;
  }
}


void IndexPage::setCmd(char *ptr)
{
  strcpy(cmd, ptr);
  strcat(cmd, "/hashindex.out");
  strcpy(globalDepthFile, ptr);
  strcat(globalDepthFile, "/globalDepth.out");
}
