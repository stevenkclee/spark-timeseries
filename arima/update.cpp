#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <pthread.h>

using namespace std;

vector<string> symbolList;
string curDate;

static void readDataList(const char* filname, vector<string> &dataList);
static string exec(string cmd);
static void *saveData(void *context);


int main()
{
	//Initialization
	vector<string> dateList;
	readDataList("./stockSymbols", symbolList);
	readDataList("./stockDate", dateList);

	curDate = dateList.back();

	int ret;
	char buf[256];
	pthread_t thread_handle[symbolList.size()];
	string cmd = "spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 --class TimeSeriesForecast ./target/arima-1.0-SNAPSHOT-jar-with-dependencies.jar 8 ";

    for(int i = 0; i < 34; ++i){
		printf("i = %d\n", i);
	 
		bzero(buf, sizeof(buf));
	    sprintf(buf, "%s%d", cmd.c_str(), i);

		cout << "Forecasting data..." << endl;
	
		ret = system("hadoop fs -rm -r /output/*");
		ret = system(buf);
		
		cout << "Saving data..." << endl;

		for(int n = 0; n <= symbolList.size() / 128 + 1; n += 128){
			int max_count = (symbolList.size() > n + 128) ? n + 128 : symbolList.size();

			for(long j = n; j < max_count; ++j)
				pthread_create(&thread_handle[j], NULL, saveData, (void*)j);

			for(long j = n; j < max_count; ++j)
				pthread_join(thread_handle[j], NULL);
		}
	}

	return 0;
}


static void readDataList(const char* filename, vector<string> &dataList)
{
	char buf[128];
	fstream file;
	file.open(filename, ios::in);

	if(!file) return;

	try{
		while(!file.eof()){
			if(file.getline(buf, 128))
				dataList.push_back(string(buf));
		}
	}
	catch(...){
		file.close();
		throw;
	}

	file.close();
}


static string exec(string cmd)
{
	char buf[128];
	string result = "";
	FILE* pipe = popen(cmd.c_str(), "r");
	
	if(!pipe)
		throw runtime_error("popen() failed");

	try{
		while(!feof(pipe)){
			if(fgets(buf, 128, pipe))
				result += buf;
		}
	}
	catch(...){
		pclose(pipe);
		throw;
	}

	pclose(pipe);
	return result;
}


static void *saveData(void *context)
{
	long i = (long)context;
	string filePath = "database/forecast/" + curDate + "_" + symbolList[i] + ".fc.csv";
	fstream file;

	file.open(filePath.c_str(), fstream::out | fstream::app);
	if(!file) pthread_exit(NULL);

	file << exec("hadoop fs -cat /output/" + curDate + "_" + symbolList[i] + ".fc.csv/part*");
	file.close();
}
