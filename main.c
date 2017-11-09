#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "config.h"
#include <cassandra.h>

void nextarg(char *ln, int *pos, char *sep, char *arg);
char *readline(char *prompt);

static int tty = 0;

char *Keyspace = NULL;
char *table = NULL;


static void
cli_about()
{
	printf("You executed a command!\n");
}

static void
cli_help()
{
	puts("This is Toshal Ghimire's Project 2");
	return;
}

static void 
cli_show(CassSession* SES)
{
	const char * query = "SELECT keyspace_name FROM system_schema.keyspaces;";
	CassStatement* statement = cass_statement_new(query,0);
	ClassFuture* query_future = cass_session_execute(SES,statement);

	if(cass_future_error_code(query_future) == CASS_OK)
	{
		const CassResult* result = cass_future_get_result(query_future);
		ClassIterator* rows = cass_iterator_from_result(result);
	
		while(cass_iterator_next(rows))
		{
		
		 const CassRow* row = cass_iterator_get_row(rows);
       		 const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

        	const char* keyspace_name;
		size_t keyspace_length; 
        	cass_value_get_string(value, &keyspace_name,&keyspace_length);
        	printf("%.*s'\n",keyspace_name);		
		
		}


		cass_result_free(result);
		cass_iterator_free(rows);
	} 
	else {
      
      	const char* message;
      	size_t message_length;
      	cass_future_error_message(result_future, &message, &message_length);
      	fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);

	}

	cass_statement_free(statement);
	cass_future_free(query_future);
return;
}

static void 
cli_list(CassSession* SES,char *key_to_list)
{
	char *base_string = "select table_name from system_schema.tables where keyspace_name = '";
	char buf[1024];

	strcpy(buf,base_string);
	strcat(buf,key_to_list);
	strcat(buf,"';")

	const char *query = buf;
	
	CassStatement* statement = cass_statement_new(query,0);
	ClassFuture* query_future = cass_session_execute(SES,statement);

	if(cass_future_error_code(query_future) == CASS_OK)
	{
		const CassResult* result = cass_future_get_result(query_future);
		ClassIterator* rows_iterator = cass_iterator_from_result(result);
	
		while(cass_iterator_next(rows_iterator))
		{
		
			const CassRow* row = cass_iterator_get_row(rows_iterator);
       		 	const CassValue* value = cass_row_get_column_by_name(row, "table_name");

        		const char* keyspace_name;
			size_t keyspace_length; 
        		cass_value_get_string(value, &keyspace_name,&keyspace_length);
        		printf("%.*s'\n",keyspace_name);		
		
		}


		cass_result_free(result);
		cass_iterator_free(rows);
	} 
	else {
      
      	const char* message;
      	size_t message_length;
      	cass_future_error_message(result_future, &message, &message_length);
      	fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);

	}

	cass_statement_free(statement);
	cass_future_free(query_future);	

	return;
}

static void 
cli_use()
{
	return;
}

static void 
cli_get(CassSession* SES,char *query)
{
	
	CassStatement* statement = cass_statement_new(query,0);
	ClassFuture* query_future = cass_session_execute(SES,statement);

	if(cass_future_error_code(query_future) == CASS_OK)
	{
		const CassResult* result = cass_future_get_result(query_future);
		ClassIterator* rows = cass_iterator_from_result(result);
	
		while(cass_iterator_next(rows))
		{
		
		 const CassRow* row = cass_iterator_get_row(rows);
       		 const CassValue* value = cass_row_get_column_by_name(row, "first_name");

        	const char* keyspace_name;
		size_t keyspace_length; 
        	cass_value_get_string(value, &keyspace_name,&keyspace_length);
        	printf("%.*s'\n",keyspace_name);		
		
		}


		cass_result_free(result);
		cass_iterator_free(rows);
	} 
	else {
      
      	const char* message;
      	size_t message_length;
      	cass_future_error_message(result_future, &message, &message_length);
      	fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);

	}

	cass_statement_free(statement);
	cass_future_free(query_future);

return;
}

static void 
cli_insert()
{
	return;
}

void
cli(CassSession* SES)
{
	char *cmdline = NULL;
	char cmd[BUFSIZE], prompt[BUFSIZE];
	int pos;

	tty = isatty(STDIN_FILENO);
	if (tty)
		cli_about();

	/* Main command line loop */
	for (;;) {
		if (cmdline != NULL) {
			free(cmdline);
			cmdline = NULL;
		}
		memset(prompt, 0, BUFSIZE);
		sprintf(prompt, "cassandra> ");

		if (tty)
			cmdline = readline(prompt);
		else
			cmdline = readline("");

		if (cmdline == NULL)
			continue;

		if (strlen(cmdline) == 0)
			continue;

		if (!tty)
			printf("%s\n", cmdline);

		if ( (strcmp(cmdline, "show") || strcmp(cmdline, "describe") )  == 0) {
			cli_show(SES);
			continue;
		}

		if ( (strcmp(cmdline, "list") || strcmp(cmdline, "LIST") )  == 0) {

			if(keyspace == NULL)
			{

			puts("Please specify keyspace first using the USE command")

			}else{

			cli_show(SES,keyspace);
			continue;
			}
		}		
		
		if (strcmp(cmdline, "?") == 0) {
			cli_help();
			continue;
		}

		if (strcmp(cmdline, "use ",4) == 0) {
			char* token_use;
			
			token_use = strtok(cmdline + 4, ".");
			keyspace = token_use;
			
			token_use = strtok(NULL, ".");
			table = token_use;
			
			
			continue;
		}

		if (strcmp(cmdline, "get ",4) == 0) {
			char* select = "select * from ";
			char* where = " where userid ='";
			char* last ="';";
			
			char buf[1024];

			strcpy(buf,select);

			strcat(buf,keyspace);
			strcat(buf,".");
			strcat(buf,table);

			strcat(buf,where);

			char* token_get;
			
			token_get = strtok(cmdline + 4, " ");
			
			
			strcat(buf,token_get);
			strcat(buf,"';")
			
			const char* query_use = buf;

			cli_use(SES,query);
			continue;
		}		
		
		if (strcmp(cmdline, "quit") == 0 ||
		    strcmp(cmdline, "q") == 0)
			break;

		memset(cmd, 0, BUFSIZE);
		pos = 0;
		nextarg(cmdline, &pos, " ", cmd);

		if (strcmp(cmd, "about") == 0 || strcmp(cmd, "a") == 0) {
			cli_about();
			continue;

		}
	}
}

int
main(int argc, char**argv)
{
  
	CassFuture* connect_future = NULL;
  	CassCluster* cluster = cass_cluster_new();
  	CassSession* session = cass_session_new();

	cass_cluster_set_contact_points(cluster, "127.0.0.1");
	
  	connect_future = cass_session_connect(session, cluster);
	
	if(cross_future_error_code(connect_future) == CASS_OK)
	{
	//run query 
	}//else error

	
	cli(session);
	

	cass_future_free(connect_future);
	cass_session_free(session);
  	cass_cluster_free(cluster);
  	
	exit(0);
}
