#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"
#include <string.h>
#include "CUnit/Basic.h"
#include "CUnit/CUnit.h"
#include <stdlib.h>

/* Pointer to the file used by the tests. */
static FILE* temp_file = NULL;

/* The suite initialization function.
 * Opens the temporary file used by the tests.
 * Returns zero on success, non-zero otherwise.
 */
int init_suite1(void)
{
   if (NULL == (temp_file = fopen("temp.txt", "w+"))) {
      return -1;
   }
   else {
      return 0;
   }
}

/* The suite cleanup function.
 * Closes the temporary file used by the tests.
 * Returns zero on success, non-zero otherwise.
 */
int clean_suite1(void)
{
   if (0 != fclose(temp_file)) {
      return -1;
   }
   else {
      temp_file = NULL;
      return 0;
   }
}
void testOneRelationJoin(void)
{
	uint32_t size_A=100;
	int32_t A[size_A];
	uint32_t size_B=100;
	int32_t B[size_B];
	resultList* resList;
	oneColumnRelation *S,*R;
	if (NULL != temp_file)
	{
        rewind(temp_file);
        ////////////////////////////////////////////////////0-99 --- 0-99 -->100//////////////////////////////////////////////////

    	FILE *fp;
    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = sameRelationJoin(R,S);
		CU_ASSERT(100== resList->numberOfResults);
        ////////////////////////////////////////////////////1 --- 0-99 -->1//////////////////////////////////////////////////


    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",1);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = sameRelationJoin(R,S);
		CU_ASSERT(1== resList->numberOfResults);
        ////////////////////////////////////////////////////1 --- 1 -->1//////////////////////////////////////////////////


    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",1);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",1);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = sameRelationJoin(R,S);
		CU_ASSERT(100== resList->numberOfResults);
        ////////////////////////////////////////////////////0-99 --- 99-0 -->0//////////////////////////////////////////////////
    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",size_B-i-1);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = sameRelationJoin(R,S);
		CU_ASSERT(0== resList->numberOfResults);

	}
}
void testRadixHashJoin(void)
{


	uint32_t size_A=100;
	int32_t A[size_A];
	uint32_t size_B=100;
	int32_t B[size_B];
	resultList* resList;
	oneColumnRelation *S,*R;
    if (NULL != temp_file)
    {
        rewind(temp_file);
        ////////////////////////////////////////////////////0-99 --- 0-99 -->100//////////////////////////////////////////////////

    	FILE *fp;
    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = RadixHashJoin(R,S);
		CU_ASSERT(100== resList->numberOfResults);
		/////////////////////////////////////////////////////ola 1 -- ola 1 -->10.000/////////////////////////////////////////////////

    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",1);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",1);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = RadixHashJoin(R,S);
		CU_ASSERT(10000== resList->numberOfResults);

		//////////////////////////////////////////////////apo 0-99 kai 99-0 -->100 results///////////////////////////////////////////////////


    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=size_B-1;i>=0;i--)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = RadixHashJoin(R,S);
		CU_ASSERT(100== resList->numberOfResults);

		////////////////////////////////////////////////////zero results ola 1 kai ola 0/////////////////////////////////////////////////////

    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",0);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",1);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = RadixHashJoin(R,S);
		CU_ASSERT(0== resList->numberOfResults);

		//////////////////////////////////////////////////0-99 --- 0-4 -->100////////////////////////////////////////////////////////////

    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",i);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",i%5);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = RadixHashJoin(R,S);
		CU_ASSERT( 100 == resList->numberOfResults);

		//////////////////////////////////////////////////0-4 --- 0-4 -->2000////////////////////////////////////////////////////////////

    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",i%5);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",i%5);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = RadixHashJoin(R,S);
		CU_ASSERT( 2000 == resList->numberOfResults);

		//////////////////////////////////////////////////0-4 --- 0-9 -->2000////////////////////////////////////////////////////////////

    	fp=fopen("input-files/input.txt","w");
    	fprintf(fp,"%d %d\n",size_A,size_B);
    	for(int32_t i=0;i<size_A;i++)
    	{
    		fprintf(fp,"%d,",i%5);
    	}
    	fprintf(fp,"\n");
    	for(int32_t i=0;i<size_B;i++)
    	{
    		fprintf(fp,"%d,",i%10);
    	}
    	fclose(fp);

		readFile(A,&size_A,B,&size_B);
		createRelationsUT(A,size_A,B,size_B,&S,&R);
		resList = RadixHashJoin(R,S);
		CU_ASSERT( 1000 == resList->numberOfResults);
   }
}


/* The main() function for setting up and running the tests.
 * Returns a CUE_SUCCESS on successful running, another
 * CUnit error code on failure.
 */
int main()
{
   CU_pSuite pSuite = NULL;

   /* initialize the CUnit test registry */
   if (CUE_SUCCESS != CU_initialize_registry())
      return CU_get_error();

   /* add a suite to the registry */
   pSuite = CU_add_suite("Suite_1", init_suite1, clean_suite1);
   if (NULL == pSuite) {
      CU_cleanup_registry();
      return CU_get_error();
   }

   /* add the tests to the suite */
   /* NOTE - ORDER IS IMPORTANT - MUST TEST fread() AFTER fprintf() */
   if ((NULL == CU_add_test(pSuite, "test of RadixHashJoin()", testRadixHashJoin))
		   ||(NULL == CU_add_test(pSuite, "test of SameRelationJoin()", testOneRelationJoin)))
   {
      CU_cleanup_registry();
      return CU_get_error();
   }

   /* Run all tests using the CUnit Basic interface */
   CU_basic_set_mode(CU_BRM_VERBOSE);
   CU_basic_run_tests();
   CU_cleanup_registry();
   return CU_get_error();
}


