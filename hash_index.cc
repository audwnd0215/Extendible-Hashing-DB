#include <cstdio>
#include <cstring>
#include <cstdlib>
#include "hash_index.h"

extern long IOcount;
extern long IOtime;

HashIndex::HashIndex()
{
  cmd[0] = '\0';
}

HashIndex::~HashIndex()
{
}

void HashIndex::createIndex()
{
  int i, cur, result;
  long orderkey, dir_num, bucketId, splitBucketId, localDepth, offsetOfRead = 0;

  IOcount = 0;	/* Reset I/O count */
  IOtime = 0;		/* Reset I/O count */
  startTime = clock();	/* get start time */
  indexMgr.createIndex();
  bucketMgr.createBucketFile();
  do{
    result = readADataPage(offsetOfRead);

    i = 0;
    while (1)
    {
      for (cur = i; i < result && dataBuf[i] != '|'; i++)
        ;
      if (i >= result)
        break;

      orderkey = toLong(dataBuf, cur, i);

      while (i < result && dataBuf[i] != '\n')
        i++;
      if (i >= result)
        break;

      dir_num = MSHash(orderkey);
      bucketId = indexMgr.getBucketId(dir_num);

      int initDepth, initDir = dir_num, cas;
      initDepth = bucketMgr.getLocalDepth(bucketId);

      do {
        cas = bucketMgr.insert(dataBuf, cur, i + 1, bucketId, splitBucketId, localDepth);
        if (cas == 0)	//  Case 0#: insert into a bucket without split
          break;

        /* All the following cases will cause bucket splitting. 
           Case 1#: split fail. The records of initial buffer will be cleared.
           Case 2#: split success and we get two nonempty buffers. 
           Case 3#: split fail. The split buffer is empty. */


        if (localDepth > indexMgr.getGlobalDepth())	// cause index doubling
        {
          indexMgr.doubling();
          // Since the global depth of index has been changed, thus the hash value should be changed too.
          dir_num = MSHash(orderkey);
        }
        indexMgr.relocation(initDir, splitBucketId, initDepth);
        bucketId = indexMgr.getBucketId(dir_num);
        initDepth++;

      } while (1);

      i++;
    }
    offsetOfRead += cur;

  } while (result == page_size);

  indexMgr.save();
  bucketMgr.save();
  endTime = clock();	/* get end time */

  printf("The time of creating index: %.3lf s\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
  printf("The number of I/O: %ld\n", IOcount);
  printf("The time of I/O shares the propotion of entire creating index time by %.2lf\%%\n", 
      (double)IOtime / (endTime - startTime) * 100);
}

int HashIndex::readADataPage(const long offsetOfRead)
{
  int result;
  FILE *pFile;
  clock_t tmpStart, tmpEnd;

  pFile = fopen(cmd, "rb");
  if (pFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }

  tmpStart = clock();
  fseek(pFile, offsetOfRead, SEEK_SET);
  result = fread(dataBuf, 1, page_size, pFile);
  fclose(pFile);
  tmpEnd = clock();
  IOtime += (tmpEnd - tmpStart);
  IOcount++;

  return result;
}

void HashIndex::setCmd(char *ptr)
{
  strcpy(cmd, ptr);
  strcat(cmd, "/lineitem.tbl");
  strcpy(queryInFile, ptr);
  strcat(queryInFile, "/testinput.in");
  strcpy(queryOutFile, ptr);
  strcat(queryOutFile, "/testoutput.out");
  indexMgr.setCmd(ptr);
  bucketMgr.setCmd(ptr);
}

void HashIndex::query()
{
  const char end[] = {'-', '1', '\n'};
  long i, queryNum, orderkey, dir_num, bucketId;
  FILE *inFile, *outFile;

  IOcount = 0;	/* Reset I/O count */
  IOtime = 0;		/* Reset I/O count */
  if ((inFile = fopen(queryInFile, "r")) == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }

  startTime = clock();	/* get start time */
  fscanf(inFile, "%ld", &queryNum);

  indexMgr.readIndex();
  bucketMgr.readBucket();
  outFile = fopen(queryOutFile, "w");	/* Create query output file */
  if (outFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  fclose(outFile);

  for (i = 0; i < queryNum; i++)
  {
    fscanf(inFile, "%ld", &orderkey);
    dir_num = MSHash(orderkey);
    bucketId = indexMgr.getBucketId(dir_num);
    bucketMgr.query(bucketId, orderkey, queryOutFile);

    outFile = fopen(queryOutFile, "a");
    if (outFile == NULL)
    {
      printf("File path error!\n");
      exit(1);
    }
    fwrite(&end, sizeof(end), 1, outFile);
    fclose(outFile);
  }
  fclose(inFile);
  endTime = clock();	/* get end time */

  double totalTime = (double)(endTime - startTime) / CLOCKS_PER_SEC;
  printf("The time of query: %.3lf s\n", totalTime);
  if (endTime > startTime)
  {
    printf("The time of I/O shares the propotion of entire query time by %.2lf\%%\n",
        (double)IOtime / (endTime - startTime));
    printf("The speed of query(the number of queries executed per second): %.2lf\n", (double)queryNum / totalTime);
  }
}
