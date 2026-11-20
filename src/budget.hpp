#pragma once
#include "authorization.hpp"
#include "database.hpp"
#include <httplib.h>
#include <condition_variable>
#include <thread>
namespace spark {
// Dates are accounting dates in Perth (UTC+8); monetary amounts are integer cents.
std::string budget_today();
int64_t budget_day(const std::string&);
std::string budget_date(int64_t);
std::string budget_next(const nlohmann::json&);
nlohmann::json budget_validate(nlohmann::json, bool subscription);
struct BudgetConflict:std::runtime_error { explicit BudgetConflict():std::runtime_error("revision_conflict"){} };
class BudgetStore {
    DatabaseConfig config_;
public:
    explicit BudgetStore(DatabaseConfig c):config_(std::move(c)){}
    void verify() const;
    nlohmann::json create(bool,const nlohmann::json&) const;
    std::optional<nlohmann::json> get(bool,const std::string&) const;
    nlohmann::json list(bool,const nlohmann::json&) const;
    std::optional<nlohmann::json> update(bool,const std::string&,const nlohmann::json&) const;
    bool remove(bool,const std::string&,uint64_t) const;
    nlohmann::json payment(const std::string&,const nlohmann::json&) const;
    nlohmann::json forecast(const nlohmann::json&) const;
    nlohmann::json summary(const nlohmann::json&) const;
    void tick() const;
};
class BudgetScheduler {
    std::mutex mutex_; std::condition_variable wake_; bool stopping_=false; std::thread worker_;
public:
    explicit BudgetScheduler(std::shared_ptr<BudgetStore>);
    ~BudgetScheduler();
};
void configure_budget(httplib::Server&,std::shared_ptr<BudgetStore>,std::shared_ptr<Authorization>);
nlohmann::json budget_endpoints();
}
