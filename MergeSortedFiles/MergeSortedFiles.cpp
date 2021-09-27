#include <fstream>
#include <iostream>
#include <string>
#include <queue>
#include <map>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "csv.hpp"

using namespace std;

namespace {

struct NamedRow
{
    int m_fileIndex;  // the index to identify the file in input files vector
    csv::CSVRow m_row;

    NamedRow(int n, csv::CSVRow r)
    {
        m_fileIndex = n;
        m_row = r;
    }
};

class NamedRowCompareLess
{
public:
    int m_columnIndex; // the column to compare

    NamedRowCompareLess(int idx)
    : m_columnIndex(idx)
    {
    }

    bool operator()(const NamedRow& lhs, const NamedRow& rhs)
    {
        // TODO
        // comparison with double types
        double tvL = (lhs.m_row[m_columnIndex].get<double>());
        double tvR = (rhs.m_row[m_columnIndex].get<double>());
        // it's a max heap priority queue
        // instead of min heap
        if (tvL<tvR)
        {
            return false;
        }
        else if (tvL==tvR)
        {
            return lhs.m_fileIndex>=rhs.m_fileIndex;
        }
        else
        {
            return true;
        }
    }
};


void mergeFiles(std::vector<string> inputFiles, int columnIndex, std::string outputFile)
{
    std::vector<std::string> filenames = inputFiles;
    std::vector<std::shared_ptr<std::ifstream>> files;
    std::vector<std::shared_ptr<csv::CSVReader>> csvReaders;
    std::map<int, std::string> fileIndexNameMap;

    for (int i = 0; i < filenames.size(); i++)
    {
        auto dataReaderPtr = std::make_shared<csv::CSVReader>();
        // read in string
        std::string fn = filenames[i];
        auto file = std::make_shared<std::ifstream>(fn.c_str());
        std::string header;
        std::getline(*file, header);
        dataReaderPtr->feed(header + "\n");

        files.push_back(file);
        csvReaders.push_back(dataReaderPtr);
        fileIndexNameMap.emplace(i,fn);
    }

    ofstream fout(outputFile.c_str());

    csv::CSVRow row;
    std::string line;

    std::priority_queue<NamedRow, std::vector<NamedRow>, NamedRowCompareLess > data_queue(columnIndex);

    // read in first line of each file
    for (int i = 0; i < filenames.size(); i++)
    {
        std::getline(*files[i], line);
        auto dataReader = csvReaders[i];
        dataReader->feed(line + "\n");
        dataReader->read_row(row);
        NamedRow namedRow(i, row);
        data_queue.push(namedRow);
    }

    while (data_queue.size() > 0)
    {
        NamedRow curNamedRow = data_queue.top();
        data_queue.pop();
        fout << fileIndexNameMap[curNamedRow.m_fileIndex] << " : " << curNamedRow.m_row.to_json_array()<< std::endl;

        int idx = curNamedRow.m_fileIndex;

        if (!files[idx]->eof())
        {
            std::getline(*files[idx], line);
            auto dataReader = csvReaders[idx];
            dataReader->feed(line + "\n");
            dataReader->read_row(row);
            NamedRow namedRow(idx, row);
            data_queue.push(namedRow);
        }
    }

    for (auto f : files)
    {
        f->close();
    }
    fout.close();
}

}  // namespace

int main(const int argc, const char** argv)
{

    std::vector<std::string> inputFiles;
    int columnIndex;
    std::string outputFile;


    namespace po = boost::program_options;
    po::options_description desc("\nMerge multiple sorted files\n\n"
    "./MergeSortedFiles -i a.csv b.csv -c 0 -o out.txt \n\n"
    "Options"
    );
    desc.add_options()("help,h", "produce help message");
    desc.add_options()("input,i", po::value<std::vector<std::string>>(&inputFiles)->required()->multitoken(),
                       "input file or files");
    desc.add_options()("column,c", po::value<int>(&columnIndex)->required(), "the column index (0 indexed) to compare");
    desc.add_options()("output,o", po::value<std::string>(&outputFile)->required(), "output file");
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help"))
    {
        std::cout << desc << "\n";
        return 1;
    }
    po::notify(vm);

    mergeFiles(inputFiles,columnIndex,outputFile);
}
