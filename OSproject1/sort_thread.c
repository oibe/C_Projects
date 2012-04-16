#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <errno.h>

#define handle_error_en(en, msg) \
               do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

struct thread_data{
	long thread_id;
	int array[3];
	int array2[3];
};

struct node{
	char* filename;
	int size;
	struct node* next;
};

struct arraywithsize{
	int size;
	char* filename;
	int* array;
};

struct filewithsize{
	char* filename;
	int size;	
};

typedef struct arraywithsize arrayWithSize;
typedef struct filewithsize fileWithSize;

struct timeval start,temptime;
int NUM_THREADS;
pthread_mutex_t mutex;
pthread_mutex_t merge_mutex;
pthread_mutex_t chunkid_mutex;
pthread_mutex_t mergenum_mutex;
pthread_barrier_t sortbarr;
pthread_barrier_t mergbarr;
struct node* sorthead;
struct node* mergehead;
int chunkid;
int num_merged;


int run_x_times(double chunks)
{
       return ceil((log(chunks)/log(2)));
}

void incrementNumMerged(){
	pthread_mutex_lock(&mergenum_mutex);
	num_merged++;
	pthread_mutex_unlock(&mergenum_mutex);
}

 
int* read_file_into_array(char* name,int number,int offset,char* check)
{
	//~ printf("\nREADING FILE %s of size %i with offset %i\n",name,number,offset);
	FILE *fptr = fopen(name,"r");
	int c;
	int i = 0;
	char buffer[100];
	int counter= 0;
	int iter = 0;
	memset(buffer,'\0',100);
	while(iter < offset && ((c = fgetc(fptr)) != EOF))
	{
		
		if(c == ',')
		{
			iter++;
		}
	}
	//~ printf("here1\n");
	if(c == EOF)
	{
		//~ printf("really\n");
		return NULL;
	}
	/*
	while(fgets(buffer,100,fptr) != NULL)
	{
		i = 0;
		for(;i < 100; i++)
		{
			if(buffer[i]==',')
			{counter++;	}
		}
		memset(buffer,'\0',100);
	}
	int keep = counter;
	fclose(fptr);
	
	fptr = fopen(name,"r");*/
	i = 0;
	int* second = malloc(sizeof(int)*number);
	counter = 0;
	//~ printf("here2\n");
	while(number > 0 && (c = fgetc(fptr))!= EOF )
	{
		
		if(c != ',')
		{
			buffer[i]= c;
			i++;
		}
		else if(c == ',')
		{
			
			second[counter] = atoi(buffer);
			counter++; 
			if(check == NULL)
			{
				number--;
			}	
			memset(buffer,'\0',100);
			i = 0;
		}
		else
		{
			printf("something else is here\n");
		}
		
	}
	//~ printf("here3\n");
	second[counter]=atoi(buffer);
	//~ printf("%x mem\n",fptr);
	//~ fclose(fptr);
	return second;
	
	
	/*for(i = 0;i < keep+1;i++)
	{
		printf(" %lf ",second[i]);
	}
	
	return second;*/
	//bubble_sort(second,keep+1);
}

void merge_file(char* fileone, char* filetwo, char* filename)
{
	//char* name ="chunk_";
	
	//har* name = malloc(sizeof(char)*10);
	//printf("%s",cid);
	//name =strcpy(name,"chunk_");
	//name = strcat(name,id);
	incrementNumMerged();
	//~ printf("\nMERGING FILE %s,%s\n",fileone,filetwo);
	char* tempname = malloc(sizeof(char)*(strlen(filename)+5));
	sprintf(tempname,"temp%s",filename);
	FILE *fptr = fopen(fileone,"r");
	FILE *fptr_output = fopen(tempname,"w");
	int c;
	int i = 0;
	char buffer[100];
	int counter= 0;
	int extra_counter = 0;
	i = 0;
	counter = 0;
	memset(buffer,'\0',100);
	for(;extra_counter < 2;extra_counter++)
	{
	
	while((c = fgetc(fptr))!= EOF )
	{
		
		if(c != ',')
		{
			buffer[i]= c;
			i++;
		}
		else if(c == ',')
		{
			
			fprintf(fptr_output,"%s,",buffer);
			memset(buffer,'\0',100);
			i = 0;
			
		}
		else
		{
			//~ printf("something else is here\n");
		}
		
	}
	
	
	if(extra_counter == 0)
	{
	fprintf(fptr_output,"%s,",buffer);
	}
	else
	{
	fprintf(fptr_output,"%s",buffer);
	fclose(fptr_output);
	fclose(fptr);
	remove(fileone);
	remove(filetwo);
	return;
	}
	//fprintf(fptr_output,"%s",buffer);
	memset(buffer,'\0',100);
	i = 0;
	fclose(fptr);
	fptr = fopen(filetwo,"r");
	//fclose(fptr_output);
	//fptr_output = fopen("testcases","a");
	}
	
	return;
}

void array_to_file_name(int number, int* array, char* filename)
{
	int i = 0;
	FILE *fptr = fopen(filename,"w");
	for(;i < number-1;i++)
	{
		fprintf(fptr,"%d,",array[i]);
	}
	fprintf(fptr,"%d",array[i]);
	fclose(fptr);
	return;
}

void array_to_file(int number, int* array, int chunkid)
{
	int i = 0;
	char* name = malloc(sizeof(char)*10);
	name =strcpy(name,"chunk_");
	char* temp = malloc(sizeof(char)*3);
	sprintf(temp,"%s%d",name, chunkid);
	
	FILE *fptr = fopen(temp,"w");
	for(;i < number-1;i++)
	{
		fprintf(fptr,"%d,",array[i]);
	}
	fprintf(fptr,"%d",array[i]);
	fclose(fptr);
	return;
}



int getchunkid(){
	pthread_mutex_lock(&chunkid_mutex);
	chunkid++;
	int i = chunkid;
	pthread_mutex_unlock(&chunkid_mutex);
	return i;
}



void addToMerglist(int *array, int size,char* filename){
	pthread_mutex_lock(&merge_mutex);
	struct node* temp = (struct node *)malloc(sizeof(struct node));
	temp->filename = filename;
	temp->size = size;
	temp->next = mergehead;
	mergehead = temp;
	pthread_mutex_unlock(&merge_mutex);
}


fileWithSize* getMergeFilename(){
	fileWithSize* ret = (fileWithSize*)malloc(sizeof(fileWithSize));
	if(mergehead!=NULL){
		struct node* temp = mergehead;
		mergehead = mergehead->next;
		ret->filename = temp->filename;
		ret->size = temp->size;
	}else{
		ret->filename = NULL;
		ret->size = 0;
	}
	return ret;
}
	


void addToSortList(char* filename,int size){
	pthread_mutex_lock(&mutex);
	struct node* temp = (struct node *)malloc(sizeof(struct node));	
	temp->filename=filename;
	temp->size = size;
	temp->next = sorthead;
	sorthead = temp;
	pthread_mutex_unlock(&mutex);
}



arrayWithSize* getSortArray(){
	pthread_mutex_lock(&mutex);
	char* filename;
	int size;
	if(sorthead!=NULL){
		struct node* temp = sorthead;
		sorthead = sorthead->next;
		filename = temp->filename;
		size=temp->size;
		struct arraywithsize* ret = (struct arraywithsize*)malloc(sizeof(struct arraywithsize));
		ret->size = size;
		//~ printf("file %s",filename);
		ret->filename= filename;
		ret->array = read_file_into_array(filename,size,0,NULL);
		if(ret->array==NULL){printf("You are correct");}
		pthread_mutex_unlock(&mutex);
		return ret;
	}
	pthread_mutex_unlock(&mutex);
	return NULL;

}


void *sort(void *threaddata)
{   
	arrayWithSize* sortchunk = getSortArray();
	long thread_id = (long)threaddata;
	while(sortchunk != NULL){
		gettimeofday(&temptime,NULL);
		printf("%lf thread %i starts work on %s...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,thread_id,sortchunk->filename);
		int size = sortchunk->size;
		int* array = sortchunk->array;
		int swapped;
		int i;
		do{
			swapped=0;
			for(i=0;i<size-1;i++){
				if(array[i]>array[i+1]){
					int temp = array[i];
					array[i] = array[i+1];
					array[i+1] = temp;
					swapped = 1;
				}
			}
		
		}while(swapped);
		//time_t end = gettimeofday();	
		//printf("%i,%i Sort of %s took %ld,by %ld \n", array[0],array[size-1], sortchunk->filename,(end-start), thread_id);
		array_to_file_name(size,array,sortchunk->filename);
		addToMerglist(array,size,sortchunk->filename);
		gettimeofday(&temptime,NULL);
		printf("%lf thread %i finished work on %s..\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,thread_id,sortchunk->filename);
		sortchunk = getSortArray();
	}
	//~ printf("%ld as exited\n",thread_id);
	gettimeofday(&temptime,NULL);
	printf("%lf No more work to do. Thread %i exiting...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,thread_id);
	pthread_exit(NULL);
}

 void *merge(void *threaddata){
	int thread_id = (int) threaddata;
	pthread_mutex_lock(&merge_mutex);
	fileWithSize* file1 = getMergeFilename();
	fileWithSize* file2 = getMergeFilename();
	pthread_mutex_unlock(&merge_mutex);
	 
	//~ printf("FILE1 = %s,%i\n",file1->filename,file1->size);
	while(file1->filename != NULL){
		gettimeofday(&temptime,NULL);
		printf("%lf Merge thread %i starts work on %s and %s...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,thread_id,file1->filename,file2->filename);
		int size;
		int i = getchunkid();
		time_t end;
		char* new_file = (char *)malloc(sizeof(char)*20);

		sprintf(new_file,"chunk_%i",i);
		if(file2->filename != NULL){
			merge_file(file1->filename,file2->filename,new_file);
			size  = file1->size + file2->size;
		}
		else{
			char* tempname = malloc(sizeof(char)*(strlen(file1->filename)+5));
			sprintf(tempname,"temp%s",new_file);
			rename(file1->filename,tempname);
			incrementNumMerged();			
			size = file1->size;
		}
		//~ printf("%s\n",new_file);
		addToSortList(new_file,size);
		gettimeofday(&temptime,NULL);
		printf("%lf Merge thread %i finishes work on %s and %s...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,thread_id,file1->filename,file2->filename);
		pthread_mutex_lock(&merge_mutex);
		file1 = getMergeFilename();
		file2 = getMergeFilename();
		pthread_mutex_unlock(&merge_mutex);
		//~ printf("MERGE JUST ENDED: %s\n",sorthead->filename);
	}
	//~ printf("MERGE COMPLETE\n");
	pthread_barrier_wait(&mergbarr);
	gettimeofday(&temptime,NULL);
	printf("%lf Merge thread %i exiting...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,thread_id,file1->filename,file2->filename);
	pthread_exit(NULL);
	
}

int initBarriersandMutex(int NUM_THREADS){
	if(pthread_mutex_init(&mutex, NULL))
	{
		printf("Unable to initialize a mutex\n");
		return -1;
	}
	
	if(pthread_mutex_init(&merge_mutex, NULL))
	{
		printf("Unable to initialize a mutex\n");
		return -1;
	}
	
	if(pthread_mutex_init(&mergenum_mutex, NULL))
	{
		printf("Unable to initialize a mutex\n");
		return -1;
	}
	
	if(pthread_mutex_init(&chunkid_mutex, NULL))
	{
		printf("Unable to initialize a mutex\n");
		return -1;
	}
	
	
	if(pthread_barrier_init(&sortbarr, NULL, NUM_THREADS))
	{
		printf("Could not create a barrier\n");
		return -1;
	}
    
	if(pthread_barrier_init(&mergbarr, NULL, NUM_THREADS))
	{
		printf("Could not create a barrier\n");
		return -1;
	}
}

int createchunk(int i){
	int* array = read_file_into_array("data.in",1024,1024*(i-1),"no");
	if(array!=NULL){
		int cid = i;
		array_to_file(1024,array,cid);
		char* filename = (char*) malloc(sizeof(char)*12);
		sprintf(filename,"chunk_%i",cid);	
		//~ printf("%s",filename);
		addToSortList(filename,1024);
		return 1;
	}
	return 0;
	
}

void renameTempFiles(){
	int i;
	for(i = 1;i<=num_merged;i++){
		char* tempfilename = (char*)malloc(sizeof(char)*20);
		char* filename = (char*)malloc(sizeof(char)*20);
		sprintf(filename,"chunk_%i",i);
		sprintf(tempfilename,"temp%s",filename);
		//~ printf(" RENAME = %i, FILES;: %s  to %s\n",rename(tempfilename,filename),tempfilename,filename);
		rename(tempfilename,filename);
	}	
}


int main (int argc, char *argv[])
{	
	
	NUM_THREADS = atoi(argv[1]);
	int NUM_CORES = atoi(argv[2]);
	cpu_set_t cpuset;
	chunkid = 0;
	int intial_chunks = 0;
	gettimeofday(&start, NULL);
	
	
	while(createchunk(getchunkid())){
		intial_chunks++;
	}
	
	pthread_t threads[intial_chunks];
	
	if(initBarriersandMutex(NUM_THREADS)== -1){
		return -1;
	}
	
	//~ printf("creating coreset\n");
	CPU_ZERO(&cpuset);
	int j;
	for (j = 0; j < NUM_CORES; j++)
	{
               CPU_SET(j, &cpuset);
	}
	//~ printf("coreset created\n");
	int rc;
	int s;
	long t;
	int n;
	int mergthreads;
	mergthreads=intial_chunks;
	for(n=0;n<run_x_times(intial_chunks)+1;n++){
		
		if(n==0){
			if(NUM_THREADS > intial_chunks){
				NUM_THREADS = intial_chunks;
			}
		}
		else if(NUM_THREADS > mergthreads){
			NUM_THREADS = mergthreads;
		}
		mergthreads = mergthreads/2;
		
		//~ printf("About to create sort threads\n");
		gettimeofday(&temptime,NULL);
		printf("%lf Creating %i Sort Threads...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,NUM_THREADS);
		printf("Binding threads to %d CPU cores...\n",NUM_CORES);
		for(t=0; t<NUM_THREADS; t++){
			//~ printf("In main: creating Sort thread %ld\n", t);
			rc = pthread_create(&threads[t], NULL, sort, (void *)t);
			s = pthread_setaffinity_np(threads[t], sizeof(cpu_set_t), &cpuset);
			//~ printf("set affinity\n");
			if (rc){
				printf("ERROR; return code from pthread_create() is %d\n", rc);
			exit(-1);
			}
		}	
	   
		for(t=0;t<NUM_THREADS;t++){
			pthread_join(threads[t],NULL);
		}   
		
		sorthead = NULL;
		
		if(n!=run_x_times(intial_chunks)){
			
			if(pthread_barrier_init(&mergbarr, NULL, mergthreads ))
			{
				printf("Could not create a barrier\n");
				return -1;
			}
			
			gettimeofday(&temptime,NULL);
			printf("%lf Merge Starting...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0);
			gettimeofday(&temptime,NULL);
			printf("%lf Creating %i Merge Threads...\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,mergthreads);
			printf("Binding threads to %d CPU cores...\n",NUM_CORES);
			num_merged = 0;
			chunkid = 0;
			if(n != run_x_times(intial_chunks) +1){ 	
				for(t=0;t<mergthreads;t++){
					//~ printf("In main: creating merge thread %ld\n", t);
					rc = pthread_create(&threads[t], NULL, merge, (void *)t);
					s = pthread_setaffinity_np(threads[t], sizeof(cpu_set_t), &cpuset);
					if (rc){
						printf("ERROR; return code from pthread_create() is %d\n", rc);
						exit(-1);
					}
				}
				
				for(t=0;t<mergthreads;t++){
					pthread_join(threads[t],NULL);
				}  
			}
			renameTempFiles();	
		}
	}
	rename("chunk_1","data_sort.out");
	
	gettimeofday(&temptime,NULL);
	printf("The program finished in %lf seconds with %i cores....\n",((temptime.tv_sec*1000000+(temptime.tv_usec))-(start.tv_sec*1000000+(start.tv_usec)))/1000000.0,NUM_CORES);
	
}
