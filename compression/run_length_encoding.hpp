#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{


template<class T>
class RunLengthEncoding : public CompressedColumn<T>{
    public:
    /***************** constructors and destructor *****************/
    RunLengthEncoding(const std::string& name, AttributeType db_type);
    virtual ~RunLengthEncoding();


    bool insert(const boost::any& new_value);
    bool insert(const T& new_value);
    //template <typename InputIterator>
    //bool insert(InputIterator first, InputIterator last);

    bool update(TID tid, const boost::any& new_value);
    bool update(PositionListPtr tid, const boost::any& new_value);

    bool remove(TID tid);
    bool remove(PositionListPtr tid);
    bool clearContent();

    const boost::any get(TID tid);
    void print() const throw();
    size_t size() const throw();
    unsigned int getSizeinBytes() const throw();

    const ColumnPtr copy() const;

    bool store(const std::string& path);
    bool load(const std::string& path);


    T& operator[](const int index);


    std::vector<T> values_;
    std::vector<int> cnt;
};



/***************** Start of Implementation Section ******************/


    template<class T>
RunLengthEncoding<T>::RunLengthEncoding(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name,db_type), values_(), cnt(){

    }

    template<class T>
    RunLengthEncoding<T>::~RunLengthEncoding(){

    }


    template<class T>
    bool RunLengthEncoding<T>::insert(const boost::any& new_value){

        if(new_value.empty()) return false;

        if(typeid(T)==new_value.type()){
             T value = boost::any_cast<T>(new_value);

             if(values_.empty()){
                 values_.push_back(value);
                 cnt.push_back(1);
                 return true;
             }
             if(values_.back()==value){
                 cnt.back()++;
                 return true;
             }
             values_.push_back(value);
             cnt.push_back(1);
             return true;
        }
        return false;
    }

    template<class T>
    bool RunLengthEncoding<T>:: insert(const T& new_value)
    {
        if(values_.empty()){
            values_.push_back(new_value);
            cnt.push_back(1);
            return true;
        }
        if(values_.back()==new_value){
            cnt.back()++;
            return true;
        }
        values_.push_back(new_value);
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
        size_t counter=0;
        for(size_t i=0;i<cnt.size();i++){
            counter+=cnt[i];
        }
        return counter;
    }


    template<class T>
    const ColumnPtr RunLengthEncoding<T>::copy() const{

        return ColumnPtr(new RunLengthEncoding<T>(*this));
    }

    template<class T>
    bool RunLengthEncoding<T>::store(const std::string& path){

        //string path("data/");
        std::string path_(path);
        path_ += "/";
        path_ += this->name_;

        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path_.c_str(),std::ios_base::binary | std::ios_base::out);
        path_ += "cnt";
        std::ofstream outfile2 (path_.c_str(),std::ios_base::binary | std::ios_base::out);

        boost::archive::binary_oarchive oa(outfile);
        boost::archive::binary_oarchive oa2(outfile2);

        oa << values_;
        oa2 << cnt;

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
        path_ += "cnt";
        std::ifstream infile2 (path_.c_str(),std::ios_base::binary | std::ios_base::in);

        boost::archive::binary_iarchive ia(infile);
        boost::archive::binary_iarchive ia2(infile2);

        ia >> values_;
        ia2 >> cnt;

        infile.close();

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

        if(typeid(T)==new_value.type()){
             T value = boost::any_cast<T>(new_value);

            int counter=0;
            while(tid > 0){
                tid-=cnt[counter];
                counter++;
            }
            if(tid=0){
                if(counter > 0 && values_[counter-1]==value){
                    cnt[counter-1]++;
                    values_.erase(counter);
                    if(values_[counter-1]==values_[counter]){
                        cnt[counter-1]+=cnt[counter];
                        cnt.erase(counter);
                        values_.erase(counter+1);
                    }
                }

                if(counter < values_.size()-2 && values_[counter+1]==value){
                    cnt[counter+1]++;
                    values_.erase(counter);
                }
                return true;
            }

            values_[counter]=value;

            if(cnt[tid]==0){
                values_.erase(values_.begin()+tid);
            }
            return this->insert(new_value);
        }
    }

    template<class T>
    bool RunLengthEncoding<T>::update(PositionListPtr tid , const boost::any& new_value){

        for(unsigned long i=0;i<tid->size();i++){
            if (!this->update((*tid)[i], new_value))
                return false;
        }
        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(TID tid){
        cnt[tid]=cnt[tid]-1;
        values_.erase(values_.begin()+tid);

        return true;
    }

    template<class T>
    bool RunLengthEncoding<T>::remove(PositionListPtr tid){
        if(!tid)
            return false;

        if(tid->empty())
            return false;

        typename PositionList::reverse_iterator rit;

        for (rit = tid->rbegin(); rit!=tid->rend(); ++rit)
            this->remove(*rit);

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
