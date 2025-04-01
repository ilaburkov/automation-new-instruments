#include "prod/funds_controller/block_trading.h"

#include "common/instrument_description/util/market_map.h"
#include "util/error/error.h"
#include "util/lexical_cast/lexical_cast.h"
#include "util/time/time.h"
#include "util/slack/slack.h"

#include <set>

namespace funds_controller {

namespace {

const std::string kTradingBlockerTable = "BLOCK_TRADING_v1";
const std::string kDoneBlockStatus = "done";
// const std::string kPendingBlockStatus = "pending";
const std::string kRemoveBlockStatus = "removed";

}  // namespace

TradingBlocker::TradingBlocker(): clickhouse_client_(getFundsControllerClickhouseClient()) {
  ASSERT_FATAL(clickhouse_client_, "Failed to create clickhouse client");
}

tl::expected<void, std::string> TradingBlocker::isTradingBlocked(
    const std::string& subaccount, const std::vector<infra::InstrumentDescription>& instruments) {
  std::string query = std::format(
      "SELECT market, symbol, type, status FROM {} WHERE subaccount = '{}'", kTradingBlockerTable, subaccount);
  infra::MarketMap<std::set<std::string>> block_assets, block_pairs;
  clickhouse_client_->Select({std::move(query)}, [&block_assets, &block_pairs](const clickhouse::Block& block) {
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      infra::Market market =
          infra::Market{util::lexical_cast<infra::Market::Type>(block[0]->As<clickhouse::ColumnString>()->At(i))};
      std::string symbol = std::string{block[1]->As<clickhouse::ColumnString>()->At(i)};
      std::string type = std::string{block[2]->As<clickhouse::ColumnString>()->At(i)};
      std::string status = std::string{block[3]->As<clickhouse::ColumnString>()->At(i)};
      if (status == kDoneBlockStatus /* || status == kPendingBlockStatus */) {
        if (type == "asset") {
          block_assets[market].insert(symbol);
        } else if (type == "pair") {
          block_pairs[market].insert(symbol);
        } else {
          ASSERT_FATAL(false, "Unknown type " + type);
        }
      }
      // LOG_DEBUG("{} {} {} {}", market, symbol, type, status);
    }
  });
  for (const auto& instrument_description : instruments) {
    auto quote_asset =
        std::string{getBaseAndQuoteAssets(instrument_description).second};
    EXPECT_WITH_STRING(
        !block_assets[instrument_description.value.market].contains(quote_asset),
        "Trading is blocked for asset " + quote_asset + " instrument " + instrument_description.value.pair);
    EXPECT_WITH_STRING(!block_pairs[instrument_description.value.market].contains(instrument_description.value.pair),
                       "Trading is blocked for pair " + instrument_description.value.pair);
  }
  return {};
}

tl::expected<void, std::string> TradingBlocker::addBlockRule(const std::string& subaccount,
                                                             infra::Market market,
                                                             const std::string& symbol,
                                                             const std::string& type) {
  EXPECT_WITH_STRING(type == "asset" || type == "pair", "Unknown type " << type);
  auto status = getStatus(subaccount, market, symbol, type);
  if (!status.has_value()) {
    LOG_INFO("insert block rule {} {} {} {}", subaccount, market, symbol, type);
    auto query = std::format(
        "INSERT INTO {} (timestamp, subaccount, market, symbol, type, status) VALUES ('{}', '{}', '{}', '{}', '{}', "
        "'{}')",
        kTradingBlockerTable,
        util::lexical_cast<std::string>(util::lexical_cast<int64_t>(nowSystem()) / 1'000'000),
        subaccount,
        util::lexical_cast<std::string>(market),
        symbol,
        type,
        kDoneBlockStatus);
    clickhouse_client_->Execute({std::move(query)});
    return {};
  }
  if (*status == kDoneBlockStatus) {
    LOG_INFO("block rule already exists");
    return {};
  }
  EXPECT_WITH_STRING(status == kRemoveBlockStatus, "Unknown status " << *status);
  EXPECT_WITH_STRING(false,
                     "Finalize table to add block rule again. Current status is "
                         << *status << " for " << subaccount << " " << market << " " << symbol << " " << type);
  return {};
}

tl::expected<void, std::string> TradingBlocker::removeBlockRule(const std::string& subaccount,
                                                                infra::Market market,
                                                                const std::string& symbol,
                                                                const std::string& type) {
  EXPECT_WITH_STRING(type == "asset" || type == "pair", "Unknown type " << type);
  auto status = getStatus(subaccount, market, symbol, type);
  if (!status.has_value()) {
    LOG_INFO("Block rule doesn't exists");
    return {};
  }
  if (*status == kRemoveBlockStatus) {
    LOG_INFO("block rule already under removal");
    return {};
  }
  EXPECT_WITH_STRING(status == kDoneBlockStatus, "Unknown status " << *status);
  LOG_INFO("remove block rule {} {} {} {}", subaccount, market, symbol, type);
  auto query = std::format(
      "ALTER TABLE {} UPDATE status = '{}' WHERE subaccount = '{}' and market = '{}' and symbol = '{}' and type = '{}'",
      kTradingBlockerTable,
      kRemoveBlockStatus,
      subaccount,
      util::lexical_cast<std::string>(market),
      symbol,
      type);
  LOG_DEBUG("{}", query);
  clickhouse_client_->Execute({std::move(query)});
  query =
      std::format("ALTER TABLE {} DELETE WHERE subaccount = '{}' and market = '{}' and symbol = '{}' and type = '{}'",
                  kTradingBlockerTable,
                  subaccount,
                  util::lexical_cast<std::string>(market),
                  symbol,
                  type);
  LOG_DEBUG("{}", query);
  clickhouse_client_->Execute({std::move(query)});
  return {};
}


std::optional<std::string> TradingBlocker::getStatus(const std::string& subaccount,
                                                     infra::Market market,
                                                     const std::string& symbol,
                                                     const std::string& type) {
  std::string query =
      std::format("SELECT status FROM {} WHERE subaccount = '{}' and market = '{}' and symbol = '{}' and type = '{}'",
                  kTradingBlockerTable,
                  subaccount,
                  util::lexical_cast<std::string>(market),
                  symbol,
                  type);
  std::optional<std::string> status = std::nullopt;
  clickhouse_client_->Select({std::move(query)}, [&](const clickhouse::Block& block) {
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      ASSERT_FATAL(i == 0,
                   "Multiple rows for block rule " << subaccount << " " << market << " " << symbol << " " << type);
      status = std::string{block[0]->As<clickhouse::ColumnString>()->At(i)};
    }
  });
  return status;
}

}  // namespace funds_controller
