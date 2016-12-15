#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>
#include <cstring>
#include <vector>

using namespace std;

vector<string> symbolList;
string curDate;

static void readDataList(const char* filname, vector<string> &dataList);

int main()
{
	vector<string> dateList;
	readDataList("./stockSymbols", symbolList);
	readDataList("./stockDate", dateList);

	curDate = dateList.back();
	
	for(int i = 0; i < symbolList.size(); ++i){
		string filePath = "./database/forecast/" + curDate + "_" + symbolList[i] + ".fc.csv";
		ofstream file;
		file.open(filePath.c_str(), ios::out);

		if(!file) continue;

		file << "time,stock,forecast" << endl;
		file.close();
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
