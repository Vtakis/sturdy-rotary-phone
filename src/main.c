#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"
#include <unistd.h>
#include <string.h>
#include <stdbool.h>

#include <errno.h>




int main(int argc,char** argv)
{




	char *filename,*paths,*onepath;
	FILE *fp;
	int fsize,i,j,k,pointer,number_of_files=1;
	multiColumnRelation *relationArray;
	all_stats *statsArray;

	char **relationFiles;
	relationFiles=readRelations(&number_of_files);

	relationArray=malloc(number_of_files*sizeof(multiColumnRelation));
	statsArray=malloc(sizeof(all_stats));


	createRelations(number_of_files,&relationArray,&statsArray,relationFiles);
//return 1;


/*	char buffer[4096];
	read(STDIN_FILENO,buffer,sizeof(buffer));
	char *token;
	token=strtok(buffer,"\n");
	while(token!=NULL){
		printf("token:%s\n",token);
		token=strtok(NULL,"\n");
	}
*/

	executeBatches(relationArray,statsArray);
	//for()



	for(i=0;i<number_of_files;i++){
		for(j=0;j<relationArray[i].colCount;j++){
			free(relationArray[i].columns[j].tuples);
		}
		free(relationArray[i].columns);
	}
	free(relationArray);

}
