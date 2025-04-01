#include "prod/funds_controller/hedge_manager.h"

#include "prod/funds_controller/block_trading.h"
#include "prod/funds_controller/main_commands.h"
#include "prod/transfer/transfer.h"

#include "common/instrument/instrument_impl.h"
#include "common/instrument_description/util/market_map.h"
#include "util/error/error.h"
#include "util/generator/generate_uuid.h"
#include "util/lexical_cast/lexical_cast.h"
#include "util/time/time.h"
#include "util/slack/slack.h"

#include <magic_enum/magic_enum.hpp>

#include <set>

namespace funds_controller {

namespace {

const std::string kHedgeTable = "FUTURES_HEDGES_v2";
const std::string kHedgeInfoTable = "HEDGES_INFO_v2";
const std::string kDoneStatus = "done";
// const std::string kPendingLoanStatus = "pending";
const std::string kRemoveStatus = "removed";

}  // namespace

HedgeManager::HedgeManager(): clickhouse_client_(getFundsControllerClickhouseClient()) {
  ASSERT_FATAL(clickhouse_client_, "Failed to create clickhouse client");
}

tl::expected<std::vector<HedgeManager::HedgeInfo>, std::string> HedgeManager::getHedgesInfo(
    const std::string& subaccount, const std::string& asset) {
  std::string query = "SELECT id, amount, initial_subaccount, hedge_id, status FROM " + kHedgeInfoTable +
      " WHERE subaccount = '" + subaccount + "' AND asset = '" + asset + "'";
  std::vector<HedgeInfo> hedges_info;
  LOG_DEBUG("{}", query);
  try {
    clickhouse_client_->Select({std::move(query)}, [&hedges_info, &subaccount, &asset](const clickhouse::Block& block) {
      for (size_t i = 0; i < block.GetRowCount(); ++i) {
        HedgeInfo hedge_info;
        hedge_info.subaccount = subaccount;
        hedge_info.asset = asset;
        hedge_info.id = convertUUIDToString(block[0]->As<clickhouse::ColumnUUID>()->At(i));
        hedge_info.amount = convertClickhouseDecimalToDecimal(block[1]->As<clickhouse::ColumnDecimal>()->At(i));
        hedge_info.initial_account = std::string{block[2]->As<clickhouse::ColumnString>()->At(i)};
        hedge_info.hedge_id = std::string{block[3]->As<clickhouse::ColumnString>()->At(i)};
        std::string status = std::string{block[4]->As<clickhouse::ColumnString>()->At(i)};
        if (status == kDoneStatus) {
          hedges_info.push_back(hedge_info);
          continue;
        }
        ASSERT_FATAL(status == kRemoveStatus, "Unknown status " << status);
      }
    });
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to get hedges info. Exception: "} + e.what());
  }
  return hedges_info;
}

tl::expected<HedgeManager::FuturesHedge, std::string> HedgeManager::getFuturesHedge(const std::string& hedge_id) {
  std::string query = "SELECT id, subaccount, market, pair, crypto_eq_amount, open_amount_usd, hedge_id, status FROM " +
      kHedgeTable + " WHERE hedge_id = '" + hedge_id + "'";
  FuturesHedge futures_hedge;
  LOG_DEBUG("{}", query);
  try {
    clickhouse_client_->Select({std::move(query)}, [&futures_hedge](const clickhouse::Block& block) {
      ASSERT_FATAL(block.GetRowCount() <= 1, "Expected only one row");
      if (block.GetRowCount() == 0) {
        return;
      }
      futures_hedge.id = convertUUIDToString(block[0]->As<clickhouse::ColumnUUID>()->At(0));
      futures_hedge.subaccount = std::string{block[1]->As<clickhouse::ColumnString>()->At(0)};
      auto market_type =
          magic_enum::enum_cast<infra::Market::Type>(std::string{block[2]->As<clickhouse::ColumnString>()->At(0)});
      if (!market_type.has_value()) {
        LOG_ERROR("Unknown market type {}", std::string{block[2]->As<clickhouse::ColumnString>()->At(0)});
        return;
      }
      futures_hedge.market = infra::Market{*market_type};
      futures_hedge.pair = std::string{block[3]->As<clickhouse::ColumnString>()->At(0)};
      futures_hedge.crypto_eq_amount =
          convertClickhouseDecimalToDecimal(block[4]->As<clickhouse::ColumnDecimal>()->At(0));
      futures_hedge.open_amount_usd =
          convertClickhouseDecimalToDecimal(block[5]->As<clickhouse::ColumnDecimal>()->At(0));
      futures_hedge.hedge_id = std::string{block[6]->As<clickhouse::ColumnString>()->At(0)};
      futures_hedge.status = std::string{block[7]->As<clickhouse::ColumnString>()->At(0)};
    });
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to get futures hedge info. Exception: "} + e.what());
  }
  return futures_hedge;
}


tl::expected<infra::Volume, std::string> HedgeManager::getCurrentHedgeAmountOnAccount(const std::string& subaccount,
                                                                                      const std::string& asset) {
  auto hedges_info = getHedgesInfo(subaccount, asset);
  PROPAGATE_ERROR(hedges_info);
  infra::Volume total_hedge_amount_on_account;
  for (const auto& hedge_info : *hedges_info) {
    total_hedge_amount_on_account += hedge_info.amount;
  }
  return total_hedge_amount_on_account;
}

tl::expected<void, std::string> HedgeManager::createHedge(const std::string& subaccount,
                                                          infra::Exchange exchange,
                                                          const std::string& asset,
                                                          infra::Volume amount) {
  EXPECT_WITH_STRING(amount > 0, "Amount should be positive");
  LOG_INFO("Creating hedge {} {} {} {} {}", subaccount, exchange, asset, amount);
  transfer::CryptoTransfer crypto_transfer({exchange});
  auto futures_instrument_description = crypto_transfer.getFuturesInstrumentByAsset(asset, exchange);
  auto instrument_updates = crypto_transfer.getInstrumentUpdates(futures_instrument_description.value.market);
  PROPAGATE_ERROR(instrument_updates);
  auto instrument_update = [&]() -> tl::expected<infra::InstrumentUpdate, std::string> {
    for (const auto& instrument_update : *instrument_updates) {
      if (instrument_update.description() == futures_instrument_description) {
        return instrument_update;
      }
    }
    EXPECT_WITH_STRING(false, "Instrument update for " << futures_instrument_description << " not found");
  }();
  PROPAGATE_ERROR(instrument_update);

  infra::InstrumentImpl instrument(*instrument_update);
  std::unique_ptr<ICommand> sell_command = std::make_unique<SendMarketCommand>(
      subaccount, futures_instrument_description, -amount * (instrument.contractSize() * (1 / instrument.lotSize())));
  std::unique_ptr<ICommand> buy_command = std::make_unique<SendMarketCommand>(
      subaccount, crypto_transfer.getSpotInstrumentByAsset(asset, exchange), amount);
  std::vector<std::unique_ptr<ICommand>> commands;
  commands.push_back(std::move(sell_command));
  commands.push_back(std::move(buy_command));
  std::unique_ptr<ICommand> command = std::make_unique<MergeCommands>(std::move(commands));
  auto hedge_result = command->execute();
  PROPAGATE_ERROR(hedge_result);
  auto process_error = [&](const std::string& error) -> tl::expected<void, std::string> {
    LOG_INFO("Closing hedge, because inserting to clickhouse failed");
    auto command_result = command->undo();
    if (!command_result.has_value()) {
      util::SlackAlerter::FundsAlerter().send("Failed to write to clickhouse and closing hedge. Closing hedge error: " +
                                       command_result.error());
      return tl::make_unexpected("Failed to write to clickhouse and closing hedge. Closing hedge error: " +
                                 command_result.error());
    }
    return tl::make_unexpected(std::string{"Failed to write to clickhouse. Exception: "} + error);
  };
  std::string hedge_id = util::generateUuid().substr(0, 30);
  auto result = createNewFuturesHedgeRow(subaccount,
                                         futures_instrument_description.value.market,
                                         futures_instrument_description.value.pair,
                                         amount,
                                         hedge_id);
  if (!result.has_value()) {
    return process_error(result.error());
  }
  result = createNewHedgeInfoRow(subaccount, asset, amount, subaccount, hedge_id);
  if (!result.has_value()) {
    // deleteRowByHedgeId(kHedgeTable, hedge_id);
    return process_error(result.error());
  }
  return {};
}

tl::expected<void, std::string> HedgeManager::deleteRowById(const std::string& table_name, const std::string& id) {
  std::string query = std::format("ALTER TABLE {} UPDATE status = '{}' WHERE id = '{}'", table_name, kRemoveStatus, id);
  try {
    clickhouse_client_->Execute({std::move(query)});
    query = std::format("ALTER TABLE {} DELETE WHERE id = '{}'", table_name, id);
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to delete row. Exception: "} + e.what());
  }
  return {};
}

tl::expected<void, std::string> HedgeManager::changeAmountInRowById(const std::string& table_name,
                                                                    const std::string& id,
                                                                    infra::Volume new_amount) {
  std::string amount_name = table_name == kHedgeTable ? "crypto_eq_amount" : "amount";
  std::string query = std::format("ALTER TABLE {} UPDATE {} = '{}' WHERE id = '{}'",
                                  table_name,
                                  amount_name,
                                  util::lexical_cast<std::string>(new_amount),
                                  id);
  try {
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to change amount in row. Exception: "} + e.what());
  }
  return {};
}


tl::expected<void, std::string> HedgeManager::createNewFuturesHedgeRow(const std::string& subaccount,
                                                                       infra::Market market,
                                                                       const std::string& pair,
                                                                       infra::Volume amount,
                                                                       const std::string& hedge_id) {
  infra::Volume amount_usd = amount *
      transfer::CryptoTransfer({market.exchange()})
          .getLastPrice(infra::InstrumentDescriptionFactory().get().create(market, pair));
  auto time = static_cast<int64_t>(nowSystem()) / 1'000'000;
  std::string query = std::format(
      "INSERT INTO {} (open_timestamp, last_update_timestamp, subaccount, market, pair, crypto_eq_amount, "
      "open_amount_usd, hedge_id, status) VALUES ('{}', "
      "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
      kHedgeTable,
      time,
      time,
      subaccount,
      magic_enum::enum_name(market.type()),
      pair,
      util::lexical_cast<std::string>(amount),
      util::lexical_cast<std::string>(amount_usd),
      hedge_id,
      kDoneStatus);
  try {
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to create new hedge row. Exception: "} + e.what());
  }
  return {};
}

tl::expected<void, std::string> HedgeManager::createNewHedgeInfoRow(const std::string& subaccount,
                                                                    const std::string& asset,
                                                                    infra::Volume amount,
                                                                    const std::string& initial_subaccount,
                                                                    const std::string& hedge_id) {
  std::string query = std::format(
      "INSERT INTO {} (timestamp, subaccount, asset, amount, initial_subaccount, type, hedge_id, status) VALUES "
      "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
      kHedgeInfoTable,
      static_cast<int64_t>(nowSystem()) / 1'000'000,
      subaccount,
      asset,
      util::lexical_cast<std::string>(amount),
      initial_subaccount,
      "hedge",
      hedge_id,
      kDoneStatus);
  try {
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to create new hedge info row. Exception: "} + e.what());
  }
  return {};
}


}  // namespace funds_controller
