/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{


/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */
template<class T>
class DictionaryCompressedColumn : public CompressedColumn<T>{
        public:
        /***************** constructors and destructor *****************/
        DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
        virtual ~DictionaryCompressedColumn();

        virtual bool insert(const boost::any& new_Value);
        virtual bool insert(const T& new_value);
        template <typename InputIterator>
        bool insert(InputIterator first, InputIterator last);

        virtual bool update(TID tid, const boost::any& new_value);
        virtual bool update(PositionListPtr tid, const boost::any& new_value);

        virtual bool remove(TID tid);
        //assumes tid list is sorted ascending
        virtual bool remove(PositionListPtr tid);
        virtual bool clearContent();

        virtual const boost::any get(TID tid);
        //virtual const boost::any* const getRawData()=0;
        virtual void print() const throw();
        virtual size_t size() const throw();
        virtual unsigned int getSizeinBytes() const throw();

        virtual const ColumnPtr copy() const;

        virtual bool store(const std::string& path);
        virtual bool load(const std::string& path);

        virtual T& operator[](const int index);

    std::vector<T> dictionary;
    std::vector<unsigned char> values;

};


/***************** Start of Implementation Section ******************/


        template<class T>
DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), dictionary(), values(){

        }

        template<class T>
        DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(){

        }

        template<class T>
    bool DictionaryCompressedColumn<T>::insert(const boost::any& new_value){

        if(new_value.empty()) return false;

        if(typeid(T)==new_value.type()){
             T value = boost::any_cast<T>(new_value);

             unsigned char entry = '0';

             if(values.empty()){
                 dictionary.push_back(value);
                 values.push_back(entry+dictionary.size()-1);
                 return true;
            }

            unsigned long i=0;
             while(i< dictionary.size() && dictionary[i]!= value){
                 i++;
             }

             if(dictionary[i]==value){
                 values.push_back(entry+i);
                 return true;
             }

             dictionary.push_back(value);
             values.push_back(entry+dictionary.size()-1);
             return true;
        }
        return false;
        }

        template<class T>
    bool DictionaryCompressedColumn<T>::insert(const T& new_value){

        unsigned char entry = '0';

        for(unsigned long i=0;i<values.size();i++){
            if(dictionary[i]==new_value){
                values.push_back(entry+i);
                return true;
            }
        }

        dictionary.push_back(new_value);
        values.push_back(entry+dictionary.size()-1);
        return true;
    }

        template<class T>
    const boost::any DictionaryCompressedColumn<T>::get(TID tid){

        if(tid<values.size()){
            return dictionary[(int) values[tid] - '0'];
        }
        return false;
        }

        template<class T>
    void DictionaryCompressedColumn<T>::print() const throw(){
            std::cout << "| " << this->name_ << " |" << std::endl;
            std::cout << "________________________" << std::endl;
            for(unsigned int i=0;i<values.size();i++){
                std::cout << "| " << dictionary[(int) values[i] - '0'] << " |" << std::endl;
        }
        }
        template<class T>
        size_t DictionaryCompressedColumn<T>::size() const throw(){
        return values.size();
        }
        template<class T>
        const ColumnPtr DictionaryCompressedColumn<T>::copy() const{
        return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
        }

        template<class T>
    bool DictionaryCompressedColumn<T>::update(TID tid, const boost::any& new_value){

        if(new_value.empty()) return false;

        if(typeid(T)==new_value.type()){
            T value = boost::any_cast<T>(new_value);
            unsigned char entry = '0';

            for(unsigned long i=0;i<dictionary.size();i++){
                if (dictionary[i]==value){
                    values[tid]=entry+i;
                    return true;
                }
            }

            dictionary.push_back(value);
            values[tid]= entry+dictionary.size()-1;
            return true;
        }
        return false;
        }

        template<class T>
    bool DictionaryCompressedColumn<T>::update(PositionListPtr tid, const boost::any& new_value){

        for(unsigned long i=0;i<tid->size();i++){
            if (!this->update((*tid)[i], new_value))
                return false;
        }
        return true;
        }

        template<class T>
    bool DictionaryCompressedColumn<T>::remove(TID tid){
        values.erase(values.begin()+tid);
        /* look if the value is used somewhere else. delete dictinary-entry in case */
        return true;
        }

        template<class T>
    bool DictionaryCompressedColumn<T>::remove(PositionListPtr tid){

        typename PositionList::reverse_iterator rit;

        for (rit = tid->rbegin(); rit!=tid->rend(); ++rit){
            this->remove(*rit);
        }

        return true;
        }

        template<class T>
        bool DictionaryCompressedColumn<T>::clearContent(){
        values.clear();

        /* maybe we could reuse the dictionary if the column is reused with same/similar data */
        dictionary.clear();
        return true;
        }


    template<class T>
    bool DictionaryCompressedColumn<T>::store(const std::string& path){

        //string path("data/");
        std::string path_(path);
        path_ += "/";
        path_ += this->name_;

        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path_.c_str(),std::ios_base::binary | std::ios_base::out);
        path_ += "dictionary";
        std::ofstream outfile2 (path_.c_str(),std::ios_base::binary | std::ios_base::out);

        boost::archive::binary_oarchive oa(outfile);
        boost::archive::binary_oarchive oa2(outfile2);

        oa << values;
        oa2 << dictionary;

        outfile.flush();
        outfile.close();

        return true;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::load(const std::string& path){

        std::string path_(path);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path_ += "/";
        path_ += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path_.c_str(),std::ios_base::binary | std::ios_base::in);
        path_ += "dictionary";
        std::ifstream infile2 (path_.c_str(),std::ios_base::binary | std::ios_base::in);

        boost::archive::binary_iarchive ia(infile);
        boost::archive::binary_iarchive ia2(infile2);

        ia >> values;
        ia2 >> dictionary;

        infile.close();

        return true;
    }

        template<class T>
    T& DictionaryCompressedColumn<T>::operator[](const int tid){
        return dictionary[(int) values[tid] - '0'];
        }

        template<class T>
        unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
        return values.capacity()*sizeof(unsigned char)+dictionary.capacity()*sizeof(T);
        }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB
