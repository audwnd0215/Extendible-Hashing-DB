#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <limits>
#include "hash_index.h"
using namespace std;

long IOcount;
long IOtime;

int main(int argc, char * argv[])
{
  HashIndex DBMS;
  int option;
  char again;

  if (argc != 2)
  {
    printf("Argument Error!\n");
    exit(1);
  }

  printf("most_8 Version\n");
  printf("Please input your option\n");
  printf("1. Create Index\n");
  printf("2. Query\n");
  printf("3. Exit\n");
  //scanf("%d", &option);

  cin >> option;

  while (1)
  {
    while (option < 1 || option > 3)
    {
      printf("Option Error! Please input your option again\n");
      scanf("%d", &option);
    }

    if (option == 3)
      break;

    DBMS.setCmd(argv[1]);
    if (option == 1)
    {
      DBMS.createIndex();
      printf("Create index success!\n\n");
    }
    else if (option == 2)
    {
      DBMS.query();
      printf("Query is completed!\n\n");
    }

    //fflush(stdin);
    cin.clear();
    cin.ignore(numeric_limits<streamsize>::max(), '\n');

    printf("Do again(y for YES and n for NO)?\n");
    cin >> again;
    if (again != 'y')
      break;
    printf("Please input your option\n");
    //scanf("%d", &option);
    cin >> option;
  }

  return 0;
}
