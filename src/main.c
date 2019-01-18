#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>

int main(int argc,char** argv)
{
	int i,j,number_of_files=1;
	multiColumnRelation *relationArray;
	all_stats *statsArray;
	char **relationFiles;

	relationFiles=readRelations(&number_of_files);
	relationArray=malloc(number_of_files*sizeof(multiColumnRelation));
	statsArray=malloc(sizeof(all_stats));
	createRelations(number_of_files,&relationArray,&statsArray,relationFiles);
	executeBatches(relationArray,statsArray);

	for(i=0;i<number_of_files;i++)
	{
		free(statsArray->array_with_stats[i]);
		free(relationFiles[i]);
	}
	free(statsArray->array_with_stats);
	free(relationFiles);
	free(statsArray->cols);
	free(statsArray);
	for(i=0;i<number_of_files;i++){
		for(j=0;j<relationArray[i].colCount;j++){
			free(relationArray[i].columns[j].tuples);
		}
		free(relationArray[i].columns);
	}
	free(relationArray);

}
