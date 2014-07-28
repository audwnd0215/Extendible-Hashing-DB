#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <set>
#include <iostream>
using namespace std;

int main()
{
  const char queryInFile[] = "lineitem_order.tbl";
  const char queryOutFile[] = "cutData.txt";
  const char testFileName[] = "testinput.in";
  long orderkey, old;
  char other[200];
  int cutTotal;
  FILE *inFile, *outFile, *testFile;
  set<long> orderkeySet;

  cutTotal = 1000000;
  testFile = fopen(testFileName, "w");
  fprintf(testFile, "%d\n", cutTotal);
  fclose(testFile);

  if ((inFile = fopen(queryInFile, "r")) == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }

  outFile = fopen(queryOutFile, "w");	/* Create query output file */
  if (outFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }
  fclose(outFile);

  outFile = fopen(queryOutFile, "a");
  if (outFile == NULL)
  {
    printf("File path error!\n");
    exit(1);
  }

  testFile = fopen(testFileName, "a");

  old = 0;
  while (orderkeySet.size() <= cutTotal)
  {
    fscanf(inFile, "%ld", &orderkey);
    fgets(other, sizeof(other), inFile);
    orderkeySet.insert(orderkey);

    if (orderkey != old)
    {
      fprintf(testFile, "%ld\n", orderkey);
      if (old != 0)
        fprintf(outFile, "-1\n");
      old = orderkey;
    }

    fprintf(outFile, "%ld", orderkey); 
    fputs(other, outFile);
  }

  fclose(inFile);
  fclose(outFile);
  fclose(testFile);

  return 0;
}
