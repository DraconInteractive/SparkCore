#pragma once
#include "budget.hpp"
namespace spark {
nlohmann::json validate_mortgage(nlohmann::json);
nlohmann::json mortgage_stats(const nlohmann::json&);
void configure_mortgage(httplib::Server&,std::shared_ptr<BudgetStore>,std::shared_ptr<Authorization>);
nlohmann::json mortgage_endpoints();
}
