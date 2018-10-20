#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"

int main(int argc,char** argv)
{
	int32_t i;
	int32_t A[]={1,2,3,4,5,10,2};
	uint32_t size_A=7;
	int32_t B[]={1,1,3};
	uint32_t size_B=3;

	relation *S,*R;

	createRelations(A,size_A,B,size_B,&S,&R);

}
