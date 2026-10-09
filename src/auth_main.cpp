#include "auth.hpp"
#include <iostream>
int main(int argc,char** argv){
    try {
        if(argc==2&&std::string(argv[1])=="--help"){
            std::cout<<"spark_credentials init FILE\nspark_credentials issue FILE device|administrator SUBJECT [PERMISSION ...]\nspark_credentials list FILE\nspark_credentials revoke FILE CREDENTIAL_ID\nspark_credentials permissions\nIssue prints the secret token once. Store it securely.\n";return 0;
        }
        if(argc==2&&std::string(argv[1])=="permissions"){std::cout<<nlohmann::json(spark::permission_names()).dump(2)<<'\n';return 0;}
        if(argc<3)throw std::runtime_error("Invalid arguments; use --help");
        std::string command=argv[1];std::filesystem::path file=argv[2];
        if(command=="init"&&argc==3)spark::initialize_credentials(file);
        else if(command=="list"&&argc==3)std::cout<<spark::list_credentials(file).dump(2)<<'\n';
        else if(command=="revoke"&&argc==4)spark::revoke_credential(file,argv[3]);
        else if(command=="issue"&&argc>=5){std::set<std::string> permissions;for(int i=5;i<argc;++i)if(!permissions.insert(argv[i]).second)throw std::runtime_error("Duplicate permission");std::cout<<spark::issue_credential(file,argv[3],argv[4],permissions).dump(2)<<std::endl;}
        else throw std::runtime_error("Invalid arguments; use --help");
        return 0;
    }catch(const std::exception& e){std::cerr<<"Credential operation failed: "<<e.what()<<'\n';return 2;}
}
