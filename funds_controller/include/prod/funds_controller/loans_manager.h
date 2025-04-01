#pragma once

#include "prod/funds_controller/clickhouse_client.h"

#include "common/instrument_description/instrument_description.h"
#include "common/types/volume.h"

#include <tl/expected.hpp>

#include <vector>

namespace funds_controller {

class LoansManager {
public:
  LoansManager();

  enum class LoanType : uint8_t {
    Normal,
    StableExchange,
    LAST = StableExchange,
  };

  struct LoanInfo {
    std::string id;
    std::string subaccount;
    std::string asset;
    infra::Volume amount;
    std::string initial_account;
    std::string loan_id;
    LoanType type;
  };

  struct BorrowInfo {
    std::string id;
    std::string asset;
    std::string subaccount;
    infra::Volume amount;
    infra::Volume open_amount_usd;
    std::string loan_id;
    std::string status;
  };

  tl::expected<infra::Volume, std::string> getCurrentLoanAmountOnAccount(const std::string& subaccount,
                                                                         const std::string& asset);

  tl::expected<std::vector<LoanInfo>, std::string> getLoansInfo(const std::string& subaccount,
                                                                const std::string& asset);

  tl::expected<BorrowInfo, std::string> getBorrowInfo(const std::string& loan_id);

  tl::expected<void, std::string> borrow(const std::string& subaccount,
                                         infra::Exchange exchange,
                                         const std::string& asset,
                                         infra::Volume amount);
  tl::expected<void, std::string> repay(const std::string& subaccount,
                                        infra::Exchange exchange,
                                        const std::string& asset,
                                        infra::Volume amount);
  tl::expected<void, std::string> transfer(const std::string& from_subaccount,
                                           infra::Exchange from_subaccount_exchange,
                                           const std::string& to_subaccount,
                                           infra::Exchange to_subaccount_exchange,
                                           const std::string& asset,
                                           infra::Volume amount);

private:
  tl::expected<void, std::string> deleteRowByLoanId(const std::string& table_name, const std::string& loan_id);

  tl::expected<void, std::string> deleteRowById(const std::string& table_name, const std::string& id);

  tl::expected<void, std::string> changeAmountInRowById(const std::string& table_name,
                                                        const std::string& id,
                                                        infra::Volume new_amount);

  tl::expected<void, std::string> createNewBorrowRow(const std::string& subaccount,
                                                     const std::string& asset,
                                                     infra::Volume amount,
                                                     const std::string& loan_id,
                                                     infra::Exchange exchange);

  tl::expected<void, std::string> createNewLoansRow(const std::string& subaccount,
                                                    const std::string& asset,
                                                    infra::Volume amount,
                                                    const std::string& initial_subaccount,
                                                    const std::string& loan_id);

  std::unique_ptr<clickhouse::Client> clickhouse_client_;
};

}  // namespace funds_controller
