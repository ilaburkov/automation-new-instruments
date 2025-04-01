#include "prod/funds_controller/block_trading.h"
#include "prod/funds_controller/loans_manager.h"
#include "prod/funds_controller/transaction_manager.h"
#include "prod/transfer/transfer.h"

#include "common/instrument/instruments_controller.h"
#include "util/argparse/argparse.h"
#include "util/env/env.h"
#include "util/lexical_cast/lexical_cast.h"
#include "util/log/log.h"

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace po = boost::program_options;

struct CommandLineArgs {};

CommandLineArgs parseArgs(int argc, char* argv[]) {
  CommandLineArgs result;

  util::argparse::ArgumentParser("Test").addLogLevelOption().parse(argc, argv);

  return result;
}

int main(int argc, char** argv) {
  quill::setupGlobal("global2", quill::LogLevel::Info);
  util::signal_handler::initDefault();

  parseArgs(argc, argv);

  funds_controller::TradingBlocker trading_blocker;
  funds_controller::LoansManager loans_manager;
  funds_controller::TransactionManager transaction_manager;
  auto result = loans_manager.borrow("sm_hft02_virtual", infra::Exchange::Binance, "BTC", 4.5);
  if (result.has_value()) {
    LOG_CRIT("Borrow was successful");
  } else {
    LOG_CRIT("{}", result.error());
    return 0;
  }
  result = loans_manager.borrow("sm_hft02_virtual", infra::Exchange::Binance, "BTC", 0.5);
  if (result.has_value()) {
    LOG_CRIT("Borrow was successful");
  } else {
    LOG_CRIT("{}", result.error());
    return 0;
  }
  auto response = loans_manager.getLoansInfo("sm_hft02_virtual", "BTC");
  if (response) {
    for (const auto& loan_info : response.value()) {
      LOG_CRIT("{} {} {} {} {} {}", loan_info.id, loan_info.subaccount, loan_info.asset, loan_info.amount,
      loan_info.initial_account, loan_info.loan_id);
    }
  } else {
    LOG_CRIT("{}", response.error());
  }
  result = loans_manager.repay("sm_hft02_virtual", infra::Exchange::Binance, "BTC", 0.6);
  if (result.has_value()) {
    LOG_CRIT("Repay was successful");
  } else {
    LOG_CRIT("{}", result.error());
    return 0;
  }
  result = transaction_manager.transfer("sm_hft02_virtual",
                                            infra::Wallet{infra::Wallet::BinancePortfolioMarginPro},
                                            "sm_hft03_virtual",
                                            infra::Wallet{infra::Wallet::BinancePortfolioMarginPro},
                                            "BTC",
                                            0.3);
  if (result.has_value()) {
    LOG_CRIT("Transfer was successful");
  } else {
    LOG_CRIT("{}", result.error());
    return 0;
  }
  response = loans_manager.getLoansInfo("sm_hft02_virtual", "BTC");
  if (response) {
    for (const auto& loan_info : response.value()) {
      LOG_CRIT("{} {} {} {} {} {}",
               loan_info.id,
               loan_info.subaccount,
               loan_info.asset,
               loan_info.amount,
               loan_info.initial_account,
               loan_info.loan_id);
    }
  } else {
    LOG_CRIT("{}", response.error());
  }
  response = loans_manager.getLoansInfo("sm_hft03_virtual", "BTC");
  if (response) {
    for (const auto& loan_info : response.value()) {
      LOG_CRIT("{} {} {} {} {} {}",
               loan_info.id,
               loan_info.subaccount,
               loan_info.asset,
               loan_info.amount,
               loan_info.initial_account,
               loan_info.loan_id);
    }
  } else {
    LOG_CRIT("{}", response.error());
  }
  trading_blocker.addBlockRule("sm_hft02_virtual", infra::Market{infra::Market::BinanceFutures}, "BTCUSDT", "pair");
  trading_blocker.removeBlockRule("sm_hft03_virtual", infra::Market{infra::Market::BinanceSpots}, "BTC", "asset");
  auto result = trading_blocker.isTradingBlocked(
      "sm_hft02_virtual",
      {infra::InstrumentDescriptionFactory::get().create(infra::Market{infra::Market::BinanceFutures}, "BTCUSDT")});
  if (result) {
    LOG_CRIT("Trading is not blocked");
  } else {
    LOG_CRIT("{}", result.error());
  }
  LOG_CRIT("Test finished");
  return 0;
}
