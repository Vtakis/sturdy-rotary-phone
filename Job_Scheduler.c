#include <pthread.h>
#include <stdio.h>
#include "Job_Scheduler.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "st_ngrams.h"

pthread_mutex_t start_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t id_counter_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t *read_mutex;
pthread_mutex_t *mutex;
pthread_cond_t *mutex_cond;
pthread_cond_t  write_cond=PTHREAD_COND_INITIALIZER;
pthread_cond_t  *read_cond;
pthread_mutex_t init= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t init1= PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t init_cond= PTHREAD_COND_INITIALIZER;
pthread_mutex_t end= PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t end_cond= PTHREAD_COND_INITIALIZER;
pthread_mutex_t cntm= PTHREAD_MUTEX_INITIALIZER;
int ending=0;
int id_counter=0;
int threads_exited=0;
int beginning=0;
int broadcast_start=0;
pthread_mutex_t threads_exited_mutex=PTHREAD_MUTEX_INITIALIZER;
int threads_reached=0;
pthread_mutex_t trm= PTHREAD_MUTEX_INITIALIZER;
void *Worker(void* j)//sunartisi ton thread akolouthei to montelo tou katanoloti paragogou
{
    int thread_mutex_place=0;
	thread_param* x=(thread_param*)j;
	int question_id=-1;
	int flag1=0;
	int start;
	while(1)
	{
        flag1=0;
        pthread_mutex_lock( &start_mutex );//pairnei tin thesi tou buffer apo tin opoia tha parei douleia
        start=x->sch->q->start;
        thread_mutex_place=start;
        start=(start+1)%(x->sch->q->size);
        x->sch->q->start=start;
        pthread_mutex_unlock( &start_mutex );
        pthread_mutex_lock( &(mutex[thread_mutex_place]));//lockarei to mutex autis tis thesis
        while(!strcmp(x->sch->q->jobs[thread_mutex_place].job_name,"-1"))//an einai adeio
        {
            if(!beginning)//an eimaste akoma stin arxi kai tora exoun dimiourgithei ta thread
            {
                pthread_mutex_lock(&(trm));
                threads_reached=threads_reached+1;//auksise autin tin mettavliti otan auti ginei osos o atrithmos ton thread kai exoun mpei oles oi douleies mesa ston buffer tote tha ta afupnisei i execute_all_jobs
                pthread_mutex_unlock(&(trm));
            }
            if(id_counter>=x->flagslen-x->sch->execution_threads)//sunthiki gia termatismo
            {
                pthread_mutex_lock( &threads_exited_mutex );
                threads_exited++;
                pthread_mutex_unlock( &threads_exited_mutex );
                if(threads_exited==x->sch->execution_threads)//an ola ta thread teleiosan steile sima stin wait_all_tasks na proxorisei
                	pthread_cond_signal(&end_cond);
            }
            pthread_cond_wait(&(read_cond[thread_mutex_place]),&(mutex[thread_mutex_place]));//koimisou mexri na se ksipnisei i execute_all_jobs
            if(ending)//edo kanoun exit ta threads
            {
                pthread_mutex_unlock( &(mutex[thread_mutex_place]));
                pthread_exit(&x->id);
            }
            if(beginning>1)//pare tin thesi tou buffer gia na ekteleseis douleia
            {
                pthread_mutex_unlock( &(mutex[thread_mutex_place]));
                pthread_mutex_lock( &start_mutex );
                start=x->sch->q->start;
                thread_mutex_place=start;
                start=(start+1)%(x->sch->q->size);
                x->sch->q->start=start;
                pthread_mutex_unlock( &start_mutex );
                pthread_mutex_lock(&(mutex[thread_mutex_place]));
            }
        }
        execute_job(x,thread_mutex_place);
        pthread_mutex_lock(&cntm);
        id_counter++;
        pthread_mutex_unlock(&cntm);
		pthread_mutex_unlock( &(mutex[thread_mutex_place]));
	}
}

void execute_job(thread_param *x,int thread_mutex_place)//voithitiki tou worker gia na ektelesei tin ergasia
{
    char* result;
    int question_id;
    question_id=x->sch->q->jobs[thread_mutex_place].id;
    if(x->sch->q->jobs[thread_mutex_place].job_fun_st!=NULL)//an einai static
    {
        result=x->sch->q->jobs[thread_mutex_place].job_fun_st(x->indx,x->sch->q->jobs[thread_mutex_place].job_work,x->hash,x->id);
    }
    else//an einai dynamic
    {
        result=x->sch->q->jobs[thread_mutex_place].job_fun_dy(x->indx,x->sch->q->jobs[thread_mutex_place].job_work,x->hash,x->sch->q->jobs[thread_mutex_place].current_version);
    }
    if(strcmp(result,"-1"))
    {
        //free(result);
        x->buffer[question_id]=result;
    }
    else if(!strcmp(result,"-1"))
    {
        x->buffer[question_id]=malloc(3);
        strcpy(x->buffer[question_id],result);
    }
    free(x->sch->q->jobs[thread_mutex_place].job_work);
    strcpy(x->sch->q->jobs[thread_mutex_place].job_name,"-1");
}

Job_Scheduler* initialize_scheduler(int execution_threads,hash_trie* indx,hash_keeper** hk,int counter,thread_param **temp)//arxikopoiisi JobScheduler
{
	Job_Scheduler* scheduler=malloc(sizeof(Job_Scheduler));
	pthread_t *workers;
	Queue* queue=malloc(sizeof(Queue));//dimiourgeia buffer
	queue->start=0;
	queue->end=0;
	queue->size=100;
	queue->jobs=malloc(queue->size*sizeof(Job));
	mutex=malloc(execution_threads*sizeof(pthread_mutex_t));//dimiourgeia mutex gia kathe thesi tou pinaka
	read_cond=malloc(execution_threads*sizeof(pthread_cond_t));//kai ena condition variable gia kathe thesi tou pinaka
	int j;
	for(j=0;j<execution_threads;j++)
	{
		pthread_mutex_init(&(mutex[j]),NULL);//arxikopoiisi ton mutex gia kathe thesi tou pinaka
		pthread_cond_init(&(read_cond[j]),NULL);//arxikopoiisi ton condition variables gia kathe thesi tou pinaka
	}
	for(j=0;j<queue->size;j++)
		strcpy(queue->jobs[j].job_name,"-1");
	scheduler->execution_threads=execution_threads;

	if (( workers = malloc ( execution_threads * sizeof ( pthread_t ))) == NULL )
	{
		perror ( " malloc ") ;
		exit (1) ;
	}
	int i;
	scheduler->q=queue;
	scheduler->tids=workers;
	int err;
	void *Worker(void*) ;
	*temp=malloc(scheduler->execution_threads*sizeof(thread_param));
	thread_param *temp1;
	temp1=*temp;
    for(i=0;i<scheduler->execution_threads;i++)//dimiourgia ton thread
	{
		temp1[i].sch=scheduler;
		temp1[i].id=i;
		temp1[i].hash=hk[i];
		temp1[i].indx=indx;
		temp1[i].buffer=NULL;
		if ((err=pthread_create( (scheduler->tids)+i , NULL , Worker ,(void*)(temp1+i))))		/*ftiaxnw ta threads workers*/
		{
			perror (" pthread_create " );
			exit (1) ;
		}
	}
	return scheduler;
}
void submit_job(Job_Scheduler* schedule,Job* j,int id,int current_version)//upovoli mias ergasias ston buffer
{
	int i;
    if(schedule->q->end==schedule->q->size)//an gemisei o buffe r tou diplasiazoume to megethos
    {
        schedule->q->size=schedule->q->size*2;
        schedule->q->jobs=realloc(schedule->q->jobs,schedule->q->size*sizeof(Job));
        int j;
        for(j=schedule->q->end;j<schedule->q->size;j++)
        {
            strcpy(schedule->q->jobs[j].job_name,"-1");
        }
    }
    schedule->q->jobs[schedule->q->end]=*j;
    schedule->q->jobs[schedule->q->end].id=id;
    if(!strcmp(schedule->q->jobs[schedule->q->end].job_name,"QS"))
    {
        schedule->q->jobs[schedule->q->end].job_fun_st=static_search;
        schedule->q->jobs[schedule->q->end].job_fun_dy=NULL;
    }
    if(!strcmp(schedule->q->jobs[schedule->q->end].job_name,"QD"))
    {
        schedule->q->jobs[schedule->q->end].job_fun_st=NULL;
        schedule->q->jobs[schedule->q->end].job_fun_dy=new_search;
        schedule->q->jobs[schedule->q->end].current_version=current_version;
    }
    schedule->q->end=schedule->q->end+1;

}

void execute_all_jobs(Job_Scheduler* schedule,thread_param *temp,char **buf_res)
{
    int i;
    threads_exited=0;
    if(!beginning)//an vriskomaste stin arxi tou programmatos kai einai i proti fora pou exei kleithei i sunartisi perimenoume mexri na dimiourgithoun ta thread kai na koimithoun giaproti fora
    {

        int flag1;
        flag1=0;
        while(1)
        {
            pthread_mutex_lock( &(trm));
            if(threads_reached==schedule->execution_threads)
            {
                flag1=1;
            }
            pthread_mutex_unlock( &(trm));
            if(flag1)
            {
                break;
            }
        }
    }
    beginning=beginning+1;
    i=0;
    for(i=0;i<5000;i++)//ta afupnoume
    {
        pthread_cond_broadcast(&(read_cond[i]));
    }
	return;
}

void wait_all_tasks_finish(Job_Scheduler* schedule)//perimenei na teleiosoun ta thread kai na tou steiloun sima na sunexisei
{
    pthread_mutex_lock(&init);
    pthread_cond_wait(&end_cond,&init);
    pthread_mutex_unlock(&init);
}
void destroy_scheduler(Job_Scheduler* schedule)
{
	free(schedule->q->jobs);
	free(schedule->tids);
}


void reset_queue(Queue *q)//arxikopoiei tin oura
{
	broadcast_start=q->end;
	q->start=0;
	q->end=0;
	id_counter=0;
}
void delete_threads(Job_Scheduler** schedule,thread_param **temp)//katharizei ton jobscheduler
{
    int i;
	int err;
	ending=1;
    for(i=0;i<5000;i++)
    {
        pthread_cond_broadcast(&(read_cond[i]));
    }
	for (i =0 ; i <(*schedule)->execution_threads ; i++)
	{
		if ( (err = pthread_join (*( (*schedule)->tids+i) , NULL )))
		{
			perror (" pthread_join " );
			exit (1) ;
		}
	}
	free((*schedule)->tids);
	free((*schedule)->q->jobs);
	free((*schedule)->q);
	for(i=0;i<5000;i++)
	{
		pthread_mutex_destroy(&mutex[i]);
		pthread_cond_destroy(&read_cond[i]);
	}
	free((*schedule));
	free((*temp));
	free(mutex);
	free(read_cond);
	pthread_mutex_destroy(&start_mutex);
}
