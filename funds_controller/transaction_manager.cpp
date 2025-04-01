#include "prod/funds_controller/transaction_manager.h"

#include "prod/funds_controller/block_trading.h"
#include "prod/transfer/transfer.h"

#include "common/instrument_description/util/market_map.h"
#include "util/error/error.h"
#include "util/lexical_cast/lexical_cast.h"
#include "util/time/time.h"
#include "util/slack/slack.h"

#include <set>

namespace funds_controller {

namespace {

const std::string kTransactionsTable = "TRANSACTIONS_v1";
const std::string kDoneStatus = "done";
const std::string kPendingStatus = "pending";
const std::string kRemoveStatus = "removed";

}  // namespace

TransactionManager::TransactionManager(): clickhouse_client_(getFundsControllerClickhouseClient()) {
  ASSERT_FATAL(clickhouse_client_, "Failed to create clickhouse client");
}

tl::expected<void, std::string> TransactionManager::transfer(const std::string& from_subaccount,
                                                             infra::Wallet from_subaccount_wallet,
                                                             const std::string& to_subaccount,
                                                             infra::Wallet to_subaccount_wallet,
                                                             const std::string& asset,
                                                             infra::Volume amount) {
  PROPAGATE_ERROR(checkAccount(from_subaccount, from_subaccount_wallet));
  PROPAGATE_ERROR(checkAccount(to_subaccount, to_subaccount_wallet));

  auto loan_amount = loans_manager_.getCurrentLoanAmountOnAccount(from_subaccount, asset);
  PROPAGATE_ERROR(loan_amount);
  infra::Volume transfer_loan_amount = util::decimal::min(loan_amount.value(), amount);
  if (infra::Wallet::marginWallet(from_subaccount_wallet.exchange()) != from_subaccount_wallet ||
      infra::Wallet::marginWallet(to_subaccount_wallet.exchange()) != to_subaccount_wallet) {
    LOG_INFO("Non margin wallet, not transfering loan");
    transfer_loan_amount = infra::Volume(0);
  }
  EXPECT_WITH_STRING(
      amount == transfer_loan_amount || from_subaccount_wallet.exchange() == to_subaccount_wallet.exchange(),
      "Can't transfer crypto (not loan) between different exchanges");
  if (transfer_loan_amount > 0) {
    auto result = loans_manager_.transfer(from_subaccount,
                                          from_subaccount_wallet.exchange(),
                                          to_subaccount,
                                          to_subaccount_wallet.exchange(),
                                          asset,
                                          transfer_loan_amount);
    PROPAGATE_ERROR(result);
  }


  if (amount - transfer_loan_amount > 0) {
    transfer::CryptoTransfer crypto_transfer({from_subaccount_wallet.exchange()});
    auto transfer_result = crypto_transfer.transfer(from_subaccount,
                                                    from_subaccount_wallet,
                                                    to_subaccount,
                                                    to_subaccount_wallet,
                                                    asset,
                                                    amount - transfer_loan_amount);
    if (!transfer_result.has_value()) {
      LOG_ERROR("Transfer failed: {}", transfer_result.error());
      if (transfer_loan_amount == 0) {
        return transfer_result;
      }
      auto result = loans_manager_.transfer(to_subaccount,
                                            to_subaccount_wallet.exchange(),
                                            from_subaccount,
                                            from_subaccount_wallet.exchange(),
                                            asset,
                                            transfer_loan_amount);
      if (!result.has_value()) {
        util::SlackAlerter::IlyaAlerter().send("Failed to transfer loan back. Error: " + result.error());
        EXPECT_WITH_STRING(false, "Failed to transfer loan back. Error: " << transfer_result.error());
      }
      return transfer_result;
    }
  }
  return addTransferTransaction(
      from_subaccount, from_subaccount_wallet, to_subaccount, to_subaccount_wallet, asset, amount);
}


tl::expected<void, std::string> TransactionManager::checkAccount(const std::string& subaccount, infra::Wallet wallet) {
  switch (wallet.exchange()) {
    case infra::Exchange::Binance: {
      auto creds_it = connector::datahub::getBinanceCreds().find(subaccount);
      EXPECT_WITH_STRING(creds_it != connector::datahub::getBinanceCreds().end(),
                         "Unknown binance subaccount " << subaccount);
      EXPECT_WITH_STRING(
          creds_it->second.sapi_api_key.has_value() || wallet.type() != infra::Wallet::BinancePortfolioMarginPro,
          "Portfolio margin pro mode is not supported for subaccount " << subaccount);
      return {};
    }
    case infra::Exchange::Bybit:
      EXPECT_WITH_STRING(connector::datahub::getBybitCreds().contains(subaccount),
                         "Unknown bybit subaccount " << subaccount);
      return {};
    case infra::Exchange::Okex:
      EXPECT_WITH_STRING(connector::datahub::getOkexCreds().contains(subaccount),
                         "Unknown okex subaccount " << subaccount);
      return {};
    default:
      EXPECT_WITH_STRING(false, "Unknown exchange " << wallet.exchange());
  }
}

tl::expected<void, std::string> TransactionManager::addTransferTransaction(const std::string& from_subaccount,
                                                                           infra::Wallet from_subaccount_wallet,
                                                                           const std::string& to_subaccount,
                                                                           infra::Wallet to_subaccount_wallet,
                                                                           const std::string& asset,
                                                                           infra::Volume amount) {
  std::string query = std::format(
      "INSERT INTO {} (timestamp, from_subaccount, from_wallet, to_subaccount, to_wallet, asset, amount, type, "
      "inner_id, status) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '0', '{}')",
      kTransactionsTable,
      static_cast<int64_t>(nowSystem()) / 1'000'000,
      from_subaccount,
      util::lexical_cast<std::string>(from_subaccount_wallet),
      to_subaccount,
      util::lexical_cast<std::string>(to_subaccount_wallet),
      asset,
      util::lexical_cast<std::string>(amount),
      "transfer",
      kDoneStatus);
  try {
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to write to clickhouse. Exception: "} + e.what());
  }
  return {};
}

}  // namespace funds_controller
