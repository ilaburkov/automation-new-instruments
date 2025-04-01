#pragma once

#include "prod/funds_controller/clickhouse_client.h"

#include "common/instrument_description/instrument_description.h"

#include <tl/expected.hpp>

#include <vector>

namespace funds_controller {

class TradingBlocker {
public:
  TradingBlocker();
  tl::expected<void, std::string> isTradingBlocked(const std::string& subaccount,
                                                   const std::vector<infra::InstrumentDescription>& instruments);
  tl::expected<void, std::string> addBlockRule(const std::string& subaccount,
                                               infra::Market market,
                                               const std::string& symbol,
                                               const std::string& type);
  tl::expected<void, std::string> removeBlockRule(const std::string& subaccount,
                                                  infra::Market market,
                                                  const std::string& symbol,
                                                  const std::string& type);

private:
  std::optional<std::string> getStatus(const std::string& subaccount,
                                       infra::Market market,
                                       const std::string& symbol,
                                       const std::string& type);
  std::unique_ptr<clickhouse::Client> clickhouse_client_;
};

}  // namespace funds_controller
