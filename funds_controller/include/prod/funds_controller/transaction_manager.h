#pragma once

#include "prod/funds_controller/loans_manager.h"

#include "common/instrument_description/instrument_description.h"
#include "common/wallet/wallet.h"

#include <tl/expected.hpp>

#include <string>
#include <vector>

namespace funds_controller {

class TransactionManager {
public:
  TransactionManager();

  tl::expected<void, std::string> transfer(const std::string& from_subaccount,
                                           infra::Wallet from_subaccount_wallet,
                                           const std::string& to_subaccount,
                                           infra::Wallet to_subaccount_wallet,
                                           const std::string& asset,
                                           infra::Volume amount);

private:
  tl::expected<void, std::string> checkAccount(const std::string& subaccount, infra::Wallet wallet);

  tl::expected<void, std::string> addTransferTransaction(const std::string& from_subaccount,
                                                         infra::Wallet from_subaccount_wallet,
                                                         const std::string& to_subaccount,
                                                         infra::Wallet to_subaccount_wallet,
                                                         const std::string& asset,
                                                         infra::Volume amount);

  std::unique_ptr<clickhouse::Client> clickhouse_client_;
  LoansManager loans_manager_;
};

}  // namespace funds_controller
