#include "google_auth.hpp"
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
using namespace spark;using nlohmann::json;
void check(bool b,const char* message){if(!b)throw std::runtime_error(message);}
std::string b64(const unsigned char* b,size_t n){std::string s(4*((n+2)/3),0);EVP_EncodeBlock((unsigned char*)s.data(),b,n);while(s.back()=='=')s.pop_back();for(auto& c:s){if(c=='+')c='-';if(c=='/')c='_';}return s;}
std::string b64(const std::string& s){return b64((const unsigned char*)s.data(),s.size());}
std::string bn(const BIGNUM* n){std::string s(BN_num_bytes(n),0);BN_bn2bin(n,(unsigned char*)s.data());return b64(s);}
std::string query(const std::string& q,const std::string& key){auto start=q.find(key+"=");check(start!=std::string::npos,"query missing");start+=key.size()+1;auto end=q.find('&',start);return q.substr(start,end==std::string::npos?end:end-start);}
struct Provider:IdentityProvider{bool valid=true;Identity owner{"admin-id","administrator","administrator",{"admin.credentials","discovery.read","files.read"}};std::optional<Identity> authenticate(const std::string& t)const override{return t=="admin"&&valid?std::optional<Identity>(owner):std::nullopt;}bool current(const Identity& i)const override{return valid&&i.credential_id==owner.credential_id;}};
httplib::Request request(const std::string& method="POST"){httplib::Request r;r.method=method;r.headers.emplace("Host","spark.example");r.headers.emplace("Origin","https://spark.example");r.headers.emplace("Content-Type","application/json");r.body="{}";return r;}
std::string cookie(const httplib::Response& r,const std::string& name){auto range=r.headers.equal_range("Set-Cookie");for(auto it=range.first;it!=range.second;++it)if(it->second.rfind(name+"=",0)==0)return it->second.substr(0,it->second.find(';'));return "";}
int main(){std::string root;try{
 char temp[]="/tmp/spark-google-test-XXXXXX";root=mkdtemp(temp);auto client=root+"/client.json",owner=root+"/owner.json";{std::ofstream f(client);f<<json{{"web",{{"client_id","test.apps.googleusercontent.com"},{"client_secret","secret"},{"redirect_uris",{"https://spark.example/auth/google/callback"}}}}};}chmod(client.c_str(),0600);
 auto provider=std::make_shared<Provider>();auto sessions=std::make_shared<BrowserSessions>(provider,"https://spark.example");
 EVP_PKEY_CTX* keyctx=EVP_PKEY_CTX_new_id(EVP_PKEY_RSA,nullptr);check(keyctx&&EVP_PKEY_keygen_init(keyctx)==1&&EVP_PKEY_CTX_set_rsa_keygen_bits(keyctx,2048)==1,"keygen init");EVP_PKEY* raw=nullptr;check(EVP_PKEY_keygen(keyctx,&raw)==1,"keygen");EVP_PKEY_CTX_free(keyctx);std::unique_ptr<EVP_PKEY,decltype(&EVP_PKEY_free)> key(raw,EVP_PKEY_free);const RSA* rsa=EVP_PKEY_get0_RSA(key.get());const BIGNUM *n,*e;RSA_get0_key(rsa,&n,&e,nullptr);
 std::string nonce,challenge,mode,subject="123456789",kid="first";int exchanges=0,key_fetches=0;
 auto transport=[&](const std::string& url,const std::string& form)->json{
  if(url=="https://www.googleapis.com/oauth2/v3/certs"){++key_fetches;return {{"keys",json::array({{{"kid",kid},{"kty","RSA"},{"alg","RS256"},{"use","sig"},{"n",bn(n)},{"e",bn(e)}}})}};}
  check(url=="https://oauth2.googleapis.com/token","unexpected endpoint");++exchanges;auto verifier=query(form,"code_verifier");unsigned char digest[32];unsigned length;EVP_Digest(verifier.data(),verifier.size(),digest,&length,EVP_sha256(),nullptr);check(b64(digest,length)==challenge,"PKCE mismatch");check(query(form,"client_secret")=="secret","missing client secret");
  if(mode=="outage")throw std::runtime_error("network failed");
  auto now=time(nullptr);json claims={{"iss","https://accounts.google.com"},{"aud","test.apps.googleusercontent.com"},{"sub",subject},{"email","owner@example.test"},{"email_verified",true},{"iat",now},{"exp",now+300},{"nonce",nonce}};
  if(mode=="audience")claims["aud"]="wrong";if(mode=="issuer")claims["iss"]="https://evil.example";if(mode=="nonce")claims["nonce"]="wrong";if(mode=="expired")claims["exp"]=now-1;if(mode=="unverified")claims["email_verified"]=false;if(mode=="future")claims["iat"]=now+120;if(mode=="azp"){claims["aud"]=json::array({"test.apps.googleusercontent.com","other"});claims["azp"]="other";}
  auto message=b64(json{{"alg",mode=="algorithm"?"HS256":"RS256"},{"kid",kid}}.dump())+"."+b64(claims.dump());EVP_MD_CTX* ctx=EVP_MD_CTX_new();check(EVP_DigestSignInit(ctx,nullptr,EVP_sha256(),nullptr,key.get())==1,"sign init");size_t size=0;check(EVP_DigestSign(ctx,nullptr,&size,(const unsigned char*)message.data(),message.size())==1,"sign size");std::string signature(size,0);check(EVP_DigestSign(ctx,(unsigned char*)signature.data(),&size,(const unsigned char*)message.data(),message.size())==1,"sign");EVP_MD_CTX_free(ctx);signature.resize(size);if(mode=="signature")signature[0]^=1;
  return {{"id_token",message+"."+b64(signature)},{"access_token","NOT_FOR_BROWSER"}};
 };
 auto google=std::make_shared<GoogleAuth>(GoogleConfig{client,owner},"https://spark.example",sessions,transport);
 auto token_login=request();token_login.headers.emplace("X-SPARK-Login","1");token_login.body=R"({"token":"admin"})";httplib::Response login;sessions->login(token_login,login);check(login.status==200,"token login");auto admin_cookie=cookie(login,session_cookie);
 auto start=[&](bool enrol){auto r=request();if(enrol)r.headers.emplace("Cookie",admin_cookie);else r.headers.emplace("X-SPARK-Login","1");r.body=R"({"remember_device":true})";httplib::Response res;google->start(r,res,enrol?std::optional<Identity>(provider->owner):std::nullopt);return res;};
 check(start(false).status==409,"login before enrolment");
 auto flow=[&](bool enrol){auto response=start(enrol);check(response.status==200,"start flow");auto url=json::parse(response.body)["authorization_url"].get<std::string>();nonce=query(url,"nonce");challenge=query(url,"code_challenge");auto r=request("GET");r.headers.erase("Origin");r.headers.emplace("Sec-Fetch-Site","cross-site");r.headers.emplace("Cookie",cookie(response,"__Host-spark_google"));r.params.emplace("state",query(url,"state"));r.params.emplace("code","one-time-code");return r;};
 auto first=flow(true);auto wrong=first;wrong.headers.erase("Cookie");httplib::Response rejected;google->callback(wrong,rejected);check(rejected.status==400&&exchanges==0,"missing browser binding accepted");httplib::Response enrolled;google->callback(first,enrolled);check(enrolled.status==200&&google->status()["enrolled"]==true,"enrolment");check(enrolled.body.find("NOT_FOR_BROWSER")==std::string::npos,"Google token leaked");
 auto google_cookie=cookie(enrolled,session_cookie);check(!google_cookie.empty(),"missing session cookie");auto read=request("GET");read.headers.emplace("Cookie",google_cookie);httplib::Response valid;auto identity=sessions->resolve(read,valid);check(identity&&identity->permissions==provider->owner.permissions,"permission escalation");check(!sessions->active(admin_cookie.substr(admin_cookie.find('=')+1),provider->owner),"old session not rotated");
 httplib::Response replay;google->callback(first,replay);check(replay.status==400&&exchanges==1,"callback replay");
 for(const auto& attack:{"audience","issuer","nonce","expired","unverified","future","azp","algorithm","signature"}){mode=attack;auto r=flow(false);httplib::Response res;google->callback(r,res);check(res.status==401,"invalid signed token accepted");check(cookie(res,session_cookie).empty(),"failure issued session");}mode="";
 subject="someone-else";auto other=flow(false);httplib::Response denied;google->callback(other,denied);check(denied.status==403,"other Google account allowed");subject="123456789";
 kid="rotated";auto rotated=flow(false);httplib::Response rotated_result;google->callback(rotated,rotated_result);check(rotated_result.status==200&&key_fetches>=2,"key rotation failed");
 provider->valid=false;httplib::Response independent;check(sessions->resolve(read,independent).has_value(),"Google depends on original token after enrolment");provider->valid=true;
 auto pending=flow(false);google->reset(false);httplib::Response revoked;sessions->session(read,revoked);check(revoked.status==401,"session revocation failed");httplib::Response stale;google->callback(pending,stale);check(stale.status==409,"stale flow accepted");
 mode="outage";auto outage=flow(false);httplib::Response unavailable;google->callback(outage,unavailable);check(unavailable.status==503,"outage handling");mode="";
 google->reset(true);check(!google->status()["enrolled"].get<bool>()&&start(false).status==409,"unlink failed");
 // An enrolment cannot outlive the administrator session which approved it.
 httplib::Response relog;sessions->login(token_login,relog);admin_cookie=cookie(relog,session_cookie);auto expired=flow(true);provider->valid=false;httplib::Response blocked;google->callback(expired,blocked);check(blocked.status==403&&!google->status()["enrolled"].get<bool>(),"revoked enrolment accepted");provider->valid=true;
 auto csrf=request();csrf.headers.emplace("X-SPARK-Login","1");csrf.headers.erase("Origin");httplib::Response bad_origin;google->start(csrf,bad_origin);check(bad_origin.status==403,"cross-origin start accepted");
 chmod(client.c_str(),0644);bool failed=false;try{validate_google_config({client,owner},"https://spark.example");}catch(...){failed=true;}check(failed,"insecure credentials file accepted");
 std::filesystem::remove_all(root);std::cout<<"Google OIDC signatures, PKCE, state/cookie binding, nonce, claims, owner restriction, permissions, rotation, replay, recovery and revocation passed\n";
 }catch(const std::exception& e){std::cerr<<e.what()<<'\n';if(!root.empty())std::filesystem::remove_all(root);return 1;}
}
