#pragma once

#include "prod/funds_controller/clickhouse_client.h"

#include "common/instrument_description/instrument_description.h"
#include "common/types/volume.h"

#include <tl/expected.hpp>

#include <vector>

namespace funds_controller {

class HedgeManager {
public:
  HedgeManager();

  struct HedgeInfo {
    std::string id;
    std::string subaccount;
    std::string asset;
    infra::Volume amount;
    std::string initial_account;
    std::string hedge_id;
    std::string status;
  };

  struct FuturesHedge {
    std::string id;
    infra::Market market = infra::Market{infra::Market::BinanceFutures};
    std::string pair;
    std::string subaccount;
    infra::Volume crypto_eq_amount;
    infra::Volume open_amount_usd;
    std::string hedge_id;
    std::string status;
  };

  tl::expected<infra::Volume, std::string> getCurrentHedgeAmountOnAccount(const std::string& subaccount,
                                                                          const std::string& asset);

  tl::expected<std::vector<HedgeInfo>, std::string> getHedgesInfo(const std::string& subaccount,
                                                                  const std::string& asset);

  tl::expected<FuturesHedge, std::string> getFuturesHedge(const std::string& hedge_id);

  tl::expected<void, std::string> createHedge(const std::string& subaccount,
                                              infra::Exchange exchange,
                                              const std::string& asset,
                                              infra::Volume amount);

  // tl::expected<void, std::string> closeHedge(const std::string& subaccount, infra::Exchange exchange, const
  // std::string& asset, infra::Volume amount);

  // tl::expected<void, std::string> transfer(const std::string& from_subaccount,
  //                                          infra::Exchange from_subaccount_exchange,
  //                                          const std::string& to_subaccount,
  //                                          infra::Exchange to_subaccount_exchange,
  //                                          const std::string& asset,
  //                                          infra::Volume amount);

private:
  tl::expected<void, std::string> deleteRowById(const std::string& table_name, const std::string& id);
  tl::expected<void, std::string> changeAmountInRowById(const std::string& table_name,
                                                        const std::string& id,
                                                        infra::Volume new_amount);

  tl::expected<void, std::string> createNewFuturesHedgeRow(const std::string& subaccount,
                                                           infra::Market market,
                                                           const std::string& pair,
                                                           infra::Volume amount,
                                                           const std::string& hedge_id);

  tl::expected<void, std::string> createNewHedgeInfoRow(const std::string& subaccount,
                                                        const std::string& asset,
                                                        infra::Volume amount,
                                                        const std::string& initial_subaccount,
                                                        const std::string& hedge_id);

  std::unique_ptr<clickhouse::Client> clickhouse_client_;
};

}  // namespace funds_controller
