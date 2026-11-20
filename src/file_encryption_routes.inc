// Included in configure_files: reuse storage descriptors, errors, and immutable revisions.
    auto crypt_handler=[storage,auth](const httplib::Request& request,httplib::Response& response){
        response.set_header("Cache-Control","private, no-store");response.set_header("Pragma","no-cache");response.set_header("X-Content-Type-Options","nosniff");
        const auto id=request.matches[1].str();
        if(!valid_id(id)){failure(response,400,"invalid_file_id");return;}
        if(!request.params.empty()){failure(response,400,"invalid_parameters");return;}
        const bool info=request.method=="GET"||request.method=="HEAD";
        // Bound simultaneous KDF allocations; requests do not queue with passwords in memory.
        static std::mutex crypt_work;
        std::unique_lock<std::mutex> work(crypt_work,std::defer_lock);
        if(!info&&!work.try_lock()){failure(response,503,"encryption_busy");return;}
        json packet;std::string password,plain;
        struct Wipe {std::string& value;~Wipe(){if(!value.empty())OPENSSL_cleanse(value.data(),value.size());}} wipe_password{password},wipe_plain{plain};
        std::filesystem::path fresh;bool committing=false;
        auto cleanup=[&]{if(!fresh.empty()&&!committing)unlink(fresh.c_str());};
        try{
            if(!info){
                auto type=request.get_header_value("Content-Type");if(type.substr(0,type.find(';'))!="application/json"){failure(response,415,"unsupported_media_type");return;}
                std::set<std::string> keys;
                packet=json::parse(request.body,[&](int depth,json::parse_event_t event,json& value){if(depth>2)throw std::invalid_argument("invalid_packet");if(event==json::parse_event_t::key&&!keys.insert(value.get<std::string>()).second)throw std::invalid_argument("duplicate_json_key");return true;});
                if(!packet.is_object())throw std::invalid_argument("invalid_packet");
                for(const auto& field:packet.items())if(field.key()!="password"&&field.key()!="mode"&&field.key()!="text")throw std::invalid_argument("unknown_field");
                if(!packet.contains("password")||!packet["password"].is_string())throw std::invalid_argument("password_required");
                password=packet["password"].get<std::string>();auto& secret=packet["password"].get_ref<std::string&>();OPENSSL_cleanse(secret.data(),secret.size());packet.erase("password");
                if(password.empty()||password.size()>1024)throw std::invalid_argument("invalid_password");
                if(request.get_header_value_count("If-Match")!=1){failure(response,428,"precondition_required");return;}
            }
            const std::string mode=info?"info":packet.value("mode",std::string{});
            const bool encrypt=request.path.size()>=8&&request.path.substr(request.path.size()-8)=="/encrypt";
            const bool edit=request.method=="PUT";
            if(!info){
                if(encrypt){if(packet.contains("mode")||packet.contains("text"))throw std::invalid_argument("unknown_field");if(password.size()<8)throw std::invalid_argument("password_too_short");}
                else if(edit){if(packet.contains("mode")||!packet.contains("text")||!packet["text"].is_string())throw std::invalid_argument("text_required");plain=packet["text"].get<std::string>();auto& text=packet["text"].get_ref<std::string&>();OPENSSL_cleanse(text.data(),text.size());packet.erase("text");if(plain.size()>2097152){failure(response,413,"text_too_large");return;}for(unsigned char c:plain)if((c<32&&c!=9&&c!=10&&c!=13)||c==127)throw std::invalid_argument("invalid_text");}
                else if((mode!="view"&&mode!="edit"&&mode!="full")||packet.contains("text"))throw std::invalid_argument("invalid_mode");
            }
            // Stable source and drive throughout transform/commit. Preview never modifies storage.
            std::unique_lock<std::shared_mutex> gate(storage->project_gate);
            storage->initialize();auto record=storage->store->get(id);if(!record){failure(response,404,"file_not_found");return;}
            const auto revision=(*record)["content_revision"].get<std::string>();const auto etag="\""+id+(revision.empty()?"":"-"+revision)+"\"";
            response.set_header("ETag",etag);
            if(!info&&request.get_header_value("If-Match")!=etag){failure(response,412,"file_changed");return;}
            Descriptor source(open(storage->content_path(*record).c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));auto state=file_cipher_info(source.fd);
            if(info){reply(response,200,{{"encrypted",state.encrypted},{"format",state.encrypted?json("SPARKENC-1"):json(nullptr)},{"plaintext_bytes",state.encrypted?json(state.plaintext_bytes):json(nullptr)}});return;}
            if(encrypt&&state.encrypted)throw std::invalid_argument("file_already_encrypted");
            if(!encrypt&&!state.encrypted)throw std::invalid_argument("file_not_encrypted");
            if(!encrypt&&!edit&&mode!="full"){
                const uint64_t limit=mode=="edit"?2097152:10485760;if(state.plaintext_bytes>limit){failure(response,413,"preview_too_large");return;}
                decrypt_file_bytes(source.fd,password,[&](const unsigned char* data,size_t n){plain.append(reinterpret_cast<const char*>(data),n);});
                if(mode=="edit"){json(plain).dump();for(unsigned char c:plain)if((c<32&&c!=9&&c!=10&&c!=13)||c==127)throw std::invalid_argument("invalid_text");}
                if(auth&&!auth->body(request,response))return;
                // All chunks authenticated before any response bytes are exposed.
                response.set_content(plain,"application/octet-stream");return;
            }
            if(edit){if(state.plaintext_bytes>2097152){failure(response,413,"text_too_large");return;}decrypt_file_bytes(source.fd,password,[](const unsigned char*,size_t){});}
            auto next=*record;auto new_revision=lease_secret();next["content_revision"]=new_revision;
            // Content revision format is 32 hexadecimal characters.
            new_revision.resize(32);next["content_revision"]=new_revision;
            const auto candidate=storage->content_path(next);Descriptor output(open(candidate.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600));fresh=candidate;uint64_t bytes=0;
            auto write_bytes=[&](const unsigned char* data,size_t n){size_t at=0;while(at<n){auto wrote=write(output.fd,data+at,n-at);if(wrote<0&&errno==EINTR)continue;if(wrote<=0)throw StorageError(wrote<0?errno:EIO);at+=wrote;}bytes+=n;};
            if(encrypt||edit){uint64_t at=0;auto read_bytes=[&](unsigned char* out,size_t n){if(edit){std::memcpy(out,plain.data()+at,n);at+=n;return;}size_t got=0;while(got<n){auto count=pread(source.fd,out+got,n-got,at+got);if(count<0&&errno==EINTR)continue;if(count<=0)throw StorageError(count<0?errno:EIO);got+=count;}at+=n;};encrypt_file_bytes(edit?plain.size():(*record)["size_bytes"].get<uint64_t>(),password,read_bytes,write_bytes);}
            else decrypt_file_bytes(source.fd,password,write_bytes);
            sync_fd(output.fd);storage->sync_directory();if(auth&&!auth->body(request,response)){cleanup();return;}
            committing=true;auto saved=storage->store->replace(id,new_revision,bytes);
            if(unlink(storage->content_path(*record).c_str())==0)storage->sync_directory();
            response.headers.erase("ETag");response.set_header("ETag","\""+id+"-"+new_revision+"\"");reply(response,200,saved);
        }catch(const json::exception&){cleanup();failure(response,400,"invalid_packet_or_text");}
        catch(const DatabaseError&){cleanup();failure(response,503,"database_unavailable");}
        catch(const std::invalid_argument& e){cleanup();const std::string code=e.what();failure(response,code=="incorrect_password_or_damaged_file"?422:code=="file_already_encrypted"||code=="file_not_encrypted"||code=="damaged_encrypted_file"||code=="unsupported_encryption_format"?409:400,e.what());}
        catch(const StorageError& e){cleanup();failure(response,e.status,e.what());}
        catch(const std::exception&){cleanup();failure(response,503,"encryption_unavailable");}
    };
    server.Get(R"(/files/([^/]+)/encryption)",crypt_handler);
    body_route(server,"POST",R"(/files/([^/]+)/encrypt)",crypt_handler,auth);
    body_route(server,"POST",R"(/files/([^/]+)/decrypt)",crypt_handler,auth);
    body_route(server,"PUT",R"(/files/([^/]+)/encrypted-content)",crypt_handler,auth,12599296);
