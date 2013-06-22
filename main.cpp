#include <string>
#include <core/global_definitions.hpp>
#include <core/base_column.hpp>
#include <core/column_base_typed.hpp>
#include <core/column.hpp>
#include <core/compressed_column.hpp>

/*this is the include for the example compressed column with empty implementation*/
#include <compression/dictionary_compressed_column.hpp>
#include <compression/run_length_encoding.hpp>

using namespace CoGaDB;

bool unittest(boost::shared_ptr<RunLengthEncoding<int> > ptr);
bool unittest(boost::shared_ptr<RunLengthEncoding<float> > ptr);
//bool unittest(boost::shared_ptr<ColumnBaseTyped<double> > ptr);
bool unittest(boost::shared_ptr<RunLengthEncoding<std::string> > ptr);

bool unittest(boost::shared_ptr<DictionaryCompressedColumn<int> > ptr);
bool unittest(boost::shared_ptr<DictionaryCompressedColumn<float> > ptr);
//bool unittest(boost::shared_ptr<ColumnBaseTyped<double> > ptr);
bool unittest(boost::shared_ptr<DictionaryCompressedColumn<std::string> > ptr);

int main(){
    /*create an object of your implemented column, and pass the smart pointer to the unittests*/
   //boost::shared_ptr<Column<int> > col (new Column<int>("int column",INT));
   boost::shared_ptr<RunLengthEncoding<int> >colRLE (new RunLengthEncoding<int>("RLE int column",INT));
   boost::shared_ptr<DictionaryCompressedColumn<int> > colDCC (new DictionaryCompressedColumn<int>("DCC column int",INT));
    if(!unittest(colRLE)){
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    if(!unittest(colDCC)){
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!" << std::endl;

   //boost::shared_ptr<Column<float> > col_float (new Column<float>("float column",FLOAT));
    boost::shared_ptr<RunLengthEncoding<float> >colRLE_float (new RunLengthEncoding<float>("RLE int column",FLOAT));
    boost::shared_ptr<DictionaryCompressedColumn<float> > colDCC_float (new DictionaryCompressedColumn<float>("DCC column int",FLOAT));
    if(!unittest(colRLE_float)){
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    if(!unittest(colDCC_float)){
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!" << std::endl;

   //boost::shared_ptr<Column<std::string> > col_string (new Column<std::string>("string column",VARCHAR));
    boost::shared_ptr<RunLengthEncoding<std::string> >colRLE_string (new RunLengthEncoding<std::string>("RLE string column",VARCHAR));
    boost::shared_ptr<DictionaryCompressedColumn<std::string> > colDCC_string (new DictionaryCompressedColumn<std::string>("DCC string column",VARCHAR));
    if(!unittest(colRLE_string)){
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    if(!unittest(colDCC_string)){
        std::cout << "At least one Unittest Failed!" << std::endl;
        return -1;
    }
    std::cout << "Unitests Passed!" << std::endl;



//	/****** BULK UPDATE TEST ******/
//	{
//		std::cout << "BULK UPDATE TEST..."; // << std::endl;
//		boost::shared_ptr<Column<int> > uncompressed_col (new Column<int>("int column",INT));
//		boost::shared_ptr<Column<int> > compressed_col (new Column<int>("int column",INT));
//		//boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));


//		uncompressed_col->insert(reference_data.begin(),reference_data.end());
//		compressed_col->insert(reference_data.begin(),reference_data.end());

//		bool result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){
//			std::cerr << std::endl << "operator== TEST FAILED!" << std::endl;
//			return false;
//		}
//		PositionListPtr tids (new PositionList());
//		int new_value=rand()%100;
//	   for(unsigned int i=0;i<10;i++){
//	 		tids->push_back(rand()%uncompressed_col->size());
//	   }
//
//		uncompressed_col->update(tids,new_value);
//		compressed_col->update(tids,new_value);

//		result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){
//			 std::cerr << std::endl << "BULK UPDATE TEST FAILED!" << std::endl;
//			 return false;
//		}
//		std::cout << "SUCCESS"<< std::endl;

//	}

//	/****** BULK DELETE TEST ******/
//	{
//		std::cout << "BULK DELETE TEST..."; // << std::endl;
//		boost::shared_ptr<Column<int> > uncompressed_col (new Column<int>("int column",INT));
//		boost::shared_ptr<Column<int> > compressed_col (new Column<int>("int column",INT));

//		//boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));

//		uncompressed_col->insert(reference_data.begin(),reference_data.end());
//		compressed_col->insert(reference_data.begin(),reference_data.end());

//		bool result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){
//			std::cerr << std::endl << "operator== TEST FAILED!" << std::endl;
//			return false;
//		}

//		PositionListPtr tids (new PositionList());

//	   for(unsigned int i=0;i<10;i++){
//	 		tids->push_back(rand()%uncompressed_col->size());
//	   }
//
//		uncompressed_col->remove(tids);
//		compressed_col->remove(tids);

//		result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){
//			 std::cerr << "BULK DELETE TEST FAILED!" << std::endl;
//			 return false;
//		}
//		std::cout << "SUCCESS"<< std::endl;

//	}


 return 0;
}

