
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::RunLengthEncoding and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{


template<class T>
class RunLengthEncoding : public CompressedColumn<T>{
    public:
    /***************** constructors and destructor *****************/
    RunLengthEncoding(const std::string& name, AttributeType db_type);
    virtual ~RunLengthEncoding();

    virtual bool insert(const boost::any& new_value)  = 0;
    virtual bool insert(const T& new_value)  = 0;
    //template <typename InputIterator>
    //bool insert(InputIterator first, InputIterator last);

    virtual bool update(TID tid, const boost::any& new_value) = 0;
    virtual bool update(PositionListPtr tid, const boost::any& new_value) = 0;

    virtual bool remove(TID tid)=0;
    //assumes tid list is sorted ascending
    virtual bool remove(PositionListPtr tid)=0;
    virtual bool clearContent()=0;

    virtual const boost::any get(TID tid) = 0;
    //virtual const boost::any* const getRawData()=0;
    virtual void print() const throw() = 0;
    virtual size_t size() const throw() = 0;
    virtual unsigned int getSizeinBytes() const throw() = 0;

    virtual const ColumnPtr copy() const = 0;

    virtual bool store(const std::string& path)  = 0;
    virtual bool load(const std::string& path)  = 0;
    virtual bool isMaterialized() const  throw();

    virtual bool isCompressed() const  throw();


    virtual T& operator[](const int index)  = 0;

    std::vector<T> values_;
    std::vector<int> cnt;

};



/***************** Start of Implementation Section ******************/


    template<class T>
    RunLengthEncoding<T>::RunLengthEncoding(const std::string& name, AttributeType db_type) : ColumnBaseTyped<T>(name,db_type){

    }

    template<class T>
    RunLengthEncoding<T>::~RunLengthEncoding(){

    }

    template<class T>
    bool RunLengthEncoding<T>::isMaterialized() const  throw(){
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::isCompressed() const  throw(){
        return true;
    }



    template<class T>
    bool RunLengthEncoding<T>::insert(const boost::any& new_value){

        if(new_value.empty()) return false;

        if(typeid(T)==new_value.type()){
             T value = boost::any_cast<T>(new_value);

             for(int i=0;i<values_.size();i++)
             {
                 if(values_[i]==new_value)
                 {
                     cnt[i]=cnt[i]+1;
                     return true;
                 }
             }

             values_.push_back(value);
             cnt.push_back(1);
             return true;
        }
        return false;
    }

    bool RunLengthEncoding<T>:: insert(const T& new_value)
    {
        for(int i=0;i<values_.size();i++)
        {
            if(values_.at(i)==new_value)
            {
                cnt.at(i)=cnt.at(i)+1;
                return true;
            }
        }

        values_.push_back(value);
        cnt.push_back(1);
        return true;
    }


    template<class T>
    const boost::any RunLengthEncoding<T>::get(TID tid){
        if(tid<values_.size())
            return boost::any(values_[tid]);
        else{
            std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid  << std::endl;
        }
        return boost::any();

    }
//TODO: cnt mit ausgeben
    template<class T>
    void RunLengthEncoding<T>::print() const throw(){

        std::cout << "| " << this->name_ << " |" << std::endl;
        std::cout << "________________________" << std::endl;
        for(unsigned int i=0;i<values_.size();i++){
            std::cout << "| " << values_[i] << " |" << std::endl;
        }

    }
    template<class T>
    size_t RunLengthEncoding<T>::size() const throw(){

        return values_.size();
    }
    template<class T>
    const ColumnPtr RunLengthEncoding<T>::copy() const{

        return ColumnPtr(new Column<T>(*this));
    }

    template<class T>
    bool RunLengthEncoding<T>::store(const std::string& path){

        //string path("data/");
        std::string path_(path);
        path_ += "/";
        path_ += this->_;
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path_.c_str(),std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << values_;

        outfile.flush();
        outfile.close();
        return true;
    }
    template<class T>
    bool RunLengthEncoding<T>::load(const std::string& path){

        std::string path_(path);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path_ += "/";
        path_ += this->name_;

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path_.c_str(),std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);
        ia >> values_;
        infile.close();


        return true;
    }
    template<class T>
    bool RunLengthEncoding<T>::isMaterialized() const  throw(){

        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::isCompressed() const  throw(){
        return true;
    }

    template<class T>
    T& RunLengthEncoding<T>::operator[](const int index){

        return values_[index];
    }

    template<class T>
    unsigned int RunLengthEncoding<T>::getSizeinBytes() const throw(){
        return values_.capacity()*sizeof(T);
    }

    template<class T>
    bool RunLengthEncoding<T>::update(TID tid, const boost::any& new_value){

        cnt[tid]=cnt[tid]-1;
        return this->insert(new_value);

    }

    template<class T>
    bool RunLengthEncoding<T>::update(PositionListPtr tid , const boost::any& new_value){

        cnt[tid]=cnt[tid]-1;
        return this->insert(new_value);

    }

    template<class T>
    bool RunLengthEncoding<T>::remove(TID tid){
        cnt[tid]=cnt[tid]-1;
        values_.erase(values_.begin()+tid);

        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(PositionListPtr tid){
        if(!tid)
            return false;
        //test whether tid list has at least one element, if not, return with error
        if(tid->empty())
            return false;

        //assert();

        typename PositionList::reverse_iterator rit;

        for (rit = tid->rbegin(); rit!=tids->rend(); ++rit)
            values_.erase(values_.begin()+(*rit));


        //brauchen wir den Mist?
        /*
        //delete tuples in reverse order, otherwise the first deletion would invalidate all other tids
        unsigned int i=tids->size()-1;
        while(true)
            TID = (*tids)[i];
            values_.erase(values_.begin()+tid);
            if(i==0) break;
        }*/


        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::clearContent(){
        values_.clear();
        cnt.clear();
        return true;
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB


