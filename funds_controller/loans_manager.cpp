#include "prod/funds_controller/loans_manager.h"

#include "prod/funds_controller/block_trading.h"
#include "prod/funds_controller/main_commands.h"
#include "prod/transfer/transfer.h"

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

const std::string kBorrowsTable = "BORROWS_v2";
const std::string kLoansInfoTable = "LOANS_INFO_v2";
const std::string kDoneLoanStatus = "done";
// const std::string kPendingLoanStatus = "pending";
const std::string kRemoveLoanStatus = "removed";

}  // namespace

class BorrowCommand : public ICommand {
public:
  BorrowCommand(const std::string& subaccount,
                infra::Exchange exchange,
                const std::string& asset,
                infra::Volume amount):
      subaccount_(subaccount), exchange_(exchange), asset_(asset), amount_(amount) {
  }

  tl::expected<void, std::string> execute() override {
    return transfer::CryptoTransfer({exchange_}).borrow(subaccount_, exchange_, asset_, amount_);
  }

  tl::expected<void, std::string> undo() override {
    return transfer::CryptoTransfer({exchange_}).repay(subaccount_, exchange_, asset_, amount_);
  }

private:
  std::string subaccount_;
  infra::Exchange exchange_;
  std::string asset_;
  infra::Volume amount_;
};

class RepayCommand : public ICommand {
public:
  RepayCommand(const std::string& subaccount, infra::Exchange exchange, const std::string& asset, infra::Volume amount):
      subaccount_(subaccount), exchange_(exchange), asset_(asset), amount_(amount) {
  }

  tl::expected<void, std::string> execute() override {
    return transfer::CryptoTransfer({exchange_}).repay(subaccount_, exchange_, asset_, amount_);
  }

  tl::expected<void, std::string> undo() override {
    return transfer::CryptoTransfer({exchange_}).borrow(subaccount_, exchange_, asset_, amount_);
  }

private:
  std::string subaccount_;
  infra::Exchange exchange_;
  std::string asset_;
  infra::Volume amount_;
};

LoansManager::LoansManager(): clickhouse_client_(getFundsControllerClickhouseClient()) {
  ASSERT_FATAL(clickhouse_client_, "Failed to create clickhouse client");
}

tl::expected<std::vector<LoansManager::LoanInfo>, std::string> LoansManager::getLoansInfo(const std::string& subaccount,
                                                                                          const std::string& asset) {
  std::string query = "SELECT id, amount, initial_subaccount, loan_id, type, status FROM " + kLoansInfoTable +
      " WHERE subaccount = '" + subaccount + "' AND asset = '" + asset + "'";
  std::vector<LoanInfo> loans_info;
  LOG_DEBUG("{}", query);
  try {
    clickhouse_client_->Select({std::move(query)}, [&loans_info, &subaccount, &asset](const clickhouse::Block& block) {
      for (size_t i = 0; i < block.GetRowCount(); ++i) {
        LoanInfo loan_info;
        loan_info.subaccount = subaccount;
        loan_info.asset = asset;
        loan_info.id = convertUUIDToString(block[0]->As<clickhouse::ColumnUUID>()->At(i));
        loan_info.amount = convertClickhouseDecimalToDecimal(block[1]->As<clickhouse::ColumnDecimal>()->At(i));
        loan_info.initial_account = std::string{block[2]->As<clickhouse::ColumnString>()->At(i)};
        loan_info.loan_id = std::string{block[3]->As<clickhouse::ColumnString>()->At(i)};
        auto loan_type = magic_enum::enum_cast<LoanType>(std::string{block[4]->As<clickhouse::ColumnString>()->At(i)});
        ASSERT_FATAL(loan_type.has_value(), "Unknown loan type " << block[4]->As<clickhouse::ColumnString>()->At(i));
        loan_info.type = loan_type.value();
        std::string status = std::string{block[5]->As<clickhouse::ColumnString>()->At(i)};
        if (status == kDoneLoanStatus) {
          loans_info.push_back(loan_info);
          continue;
        }
        ASSERT_FATAL(status == kRemoveLoanStatus, "Unknown status " << status);
      }
    });
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to get loans info. Exception: "} + e.what());
  }
  return loans_info;
}

tl::expected<LoansManager::BorrowInfo, std::string> LoansManager::getBorrowInfo(const std::string& loan_id) {
  std::string query = "SELECT id, subaccount, asset, amount, open_amount_usd, loan_id, status FROM " + kBorrowsTable +
      " WHERE loan_id = '" + loan_id + "'";
  BorrowInfo borrow_info;
  LOG_DEBUG("{}", query);
  try {
    clickhouse_client_->Select({std::move(query)}, [&borrow_info](const clickhouse::Block& block) {
      ASSERT_FATAL(block.GetRowCount() <= 1, "Expected only one row");
      if (block.GetRowCount() == 0) {
        return;
      }
      borrow_info.id = convertUUIDToString(block[0]->As<clickhouse::ColumnUUID>()->At(0));
      borrow_info.subaccount = std::string{block[1]->As<clickhouse::ColumnString>()->At(0)};
      borrow_info.asset = std::string{block[2]->As<clickhouse::ColumnString>()->At(0)};
      borrow_info.amount = convertClickhouseDecimalToDecimal(block[3]->As<clickhouse::ColumnDecimal>()->At(0));
      borrow_info.open_amount_usd = convertClickhouseDecimalToDecimal(block[4]->As<clickhouse::ColumnDecimal>()->At(0));
      borrow_info.loan_id = std::string{block[5]->As<clickhouse::ColumnString>()->At(0)};
      borrow_info.status = std::string{block[6]->As<clickhouse::ColumnString>()->At(0)};
    });
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to get borrow info. Exception: "} + e.what());
  }
  return borrow_info;
}


tl::expected<infra::Volume, std::string> LoansManager::getCurrentLoanAmountOnAccount(const std::string& subaccount,
                                                                                     const std::string& asset) {
  auto loans_info = getLoansInfo(subaccount, asset);
  PROPAGATE_ERROR(loans_info);
  infra::Volume total_loan_amount_on_account;
  for (const auto& loan_info : *loans_info) {
    total_loan_amount_on_account += loan_info.amount;
  }
  return total_loan_amount_on_account;
}

tl::expected<void, std::string> LoansManager::borrow(const std::string& subaccount,
                                                     infra::Exchange exchange,
                                                     const std::string& asset,
                                                     infra::Volume amount) {
  ASSERT_FATAL(amount > 0, "Amount should be positive");
  LOG_INFO("Borrowing {} {} {}", subaccount, asset, amount);
  std::string loan_id = util::generateUuid().substr(0, 30);
  std::unique_ptr<ICommand> borrow_command = std::make_unique<BorrowCommand>(subaccount, exchange, asset, amount);
  auto borrow_result = borrow_command->execute();
  PROPAGATE_ERROR(borrow_result);

  auto process_error = [&](const std::string& error) -> tl::expected<void, std::string> {
    LOG_INFO("repaying, because inserting to clickhouse failed");
    auto repay_result = borrow_command->undo();
    if (!repay_result.has_value()) {
       util::SlackAlerter::FundsAlerter().send("Failed to write to clickhouse and to repay. Repay error: " + repay_result.error());
      return tl::make_unexpected("Failed to write to clickhouse and to repay. Repay error: " + repay_result.error());
    }
    return tl::make_unexpected(std::string{"Failed to write to clickhouse. Exception: "} + error);
  };

  auto result = createNewBorrowRow(subaccount, asset, amount, loan_id, exchange);
  if (!result.has_value()) {
    return process_error(result.error());
  }
  result = createNewLoansRow(subaccount, asset, amount, subaccount, loan_id);
  if (!result.has_value()) {
    deleteRowByLoanId(kBorrowsTable, loan_id);
    return process_error(result.error());
  }
  return {};
}

tl::expected<void, std::string> LoansManager::repay(const std::string& subaccount,
                                                    infra::Exchange exchange,
                                                    const std::string& asset,
                                                    infra::Volume amount) {
  ASSERT_FATAL(amount > 0, "Amount should be positive");
  LOG_INFO("Repaying {} {} {} {}", subaccount, exchange, asset, amount);
  auto loans_info = getLoansInfo(subaccount, asset);
  PROPAGATE_ERROR(loans_info);
  infra::Volume total_loan_amount_on_account;
  for (const auto& loan_info : *loans_info) {
    if (loan_info.initial_account != subaccount) {
      continue;
    }
    total_loan_amount_on_account += loan_info.amount;
  }
  EXPECT_WITH_STRING(total_loan_amount_on_account >= amount, "Not enough borrowed amount to repay");
  for (const auto& loan_info : *loans_info) {
    if (loan_info.initial_account != subaccount) {
      continue;
    }
    auto borrow_info = getBorrowInfo(loan_info.loan_id);
    PROPAGATE_ERROR(borrow_info);
    EXPECT_WITH_STRING(borrow_info->status == kDoneLoanStatus, "Borrow should be done");
    EXPECT_WITH_STRING(borrow_info->amount >= loan_info.amount, "Borrow amount should be greater than loan amount");
    infra::Volume repay_amount = util::decimal::min(loan_info.amount, amount);
    std::unique_ptr<ICommand> repay_command = std::make_unique<RepayCommand>(subaccount, exchange, asset, repay_amount);
    auto repay_result = repay_command->execute();
    PROPAGATE_ERROR(repay_result);

    auto result = [&]() {
      if (loan_info.amount == repay_amount) {
        return deleteRowById(kLoansInfoTable, loan_info.id);
      }
      return changeAmountInRowById(kLoansInfoTable, loan_info.id, loan_info.amount - repay_amount);
    }();
    if (!result.has_value()) {
      auto borrow_result = repay_command->undo();
      if (!borrow_result.has_value()) {
         util::SlackAlerter::FundsAlerter().send("Failed to repay and to write to clickhouse");
        return tl::make_unexpected("Failed to repay and to write to clickhouse");
      }
      return result;
    }

    result = [&]() {
      if (borrow_info->amount == repay_amount) {
        return deleteRowById(kBorrowsTable, borrow_info->id);
      }
      return changeAmountInRowById(kBorrowsTable, borrow_info->id, borrow_info->amount - repay_amount);
    }();

    if (!result.has_value()) {
      auto borrow_result = repay_command->undo();
      if (!borrow_result.has_value()) {
         util::SlackAlerter::FundsAlerter().send("Failed to repay and to write to clickhouse");
        return tl::make_unexpected("Failed to repay and to write to clickhouse");
      }
      return result;
    }

    amount -= repay_amount;
    if (amount == 0) {
      break;
    }
    // sleep for 1 second to avoid rate limit repayment
    std::this_thread::sleep_for(1s);
  }
  return {};
}

tl::expected<void, std::string> LoansManager::transfer(const std::string& from_subaccount,
                                                       infra::Exchange from_subaccount_exchange,
                                                       const std::string& to_subaccount,
                                                       infra::Exchange to_subaccount_exchange,
                                                       const std::string& asset,
                                                       infra::Volume amount) {
  EXPECT_WITH_STRING(amount > 0, "Amount should be positive");
  LOG_INFO("Transferring {} {} {} {} {}",
           from_subaccount,
           from_subaccount_exchange,
           to_subaccount,
           to_subaccount_exchange,
           amount);
  auto make_transfer_command = [&](infra::Volume transfer_amount) -> std::unique_ptr<ICommand> {
    if (from_subaccount_exchange == to_subaccount_exchange) {
      return std::make_unique<TransferCryptoCommand>(from_subaccount,
                                                     infra::Wallet::marginWallet(from_subaccount_exchange),
                                                     to_subaccount,
                                                     infra::Wallet::marginWallet(to_subaccount_exchange),
                                                     asset,
                                                     transfer_amount);
    }

    std::unique_ptr<ICommand> sell_command = std::make_unique<SendMarketCommand>(
        from_subaccount,
        transfer::CryptoTransfer({from_subaccount_exchange}).getSpotInstrumentByAsset(asset, from_subaccount_exchange),
        -transfer_amount);
    std::unique_ptr<ICommand> buy_command = std::make_unique<SendMarketCommand>(
        to_subaccount,
        transfer::CryptoTransfer({from_subaccount_exchange}).getSpotInstrumentByAsset(asset, to_subaccount_exchange),
        transfer_amount);
    std::vector<std::unique_ptr<ICommand>> commands;
    commands.push_back(std::move(sell_command));
    commands.push_back(std::move(buy_command));
    return std::make_unique<MergeCommands>(std::move(commands));
  };
  auto loans_info = getLoansInfo(from_subaccount, asset);
  PROPAGATE_ERROR(loans_info);
  infra::Volume total_loan_amount_on_account;
  for (const auto& loan_info : *loans_info) {
    total_loan_amount_on_account += loan_info.amount;
  }
  EXPECT_WITH_STRING(total_loan_amount_on_account >= amount, "Not enough borrowed amount to repay");
  for (const auto& loan_info : *loans_info) {
    infra::Volume transfer_amount = util::decimal::min(loan_info.amount, amount);
    std::unique_ptr<ICommand> transfer_command = make_transfer_command(transfer_amount);
    auto repay_result = transfer_command->execute();
    PROPAGATE_ERROR(repay_result);

    auto result = [&]() {
      if (loan_info.amount == transfer_amount) {
        return deleteRowById(kLoansInfoTable, loan_info.id);
      }
      return changeAmountInRowById(kLoansInfoTable, loan_info.id, loan_info.amount - transfer_amount);
    }();
    if (!result.has_value()) {
      auto transfer_result = transfer_command->undo();
      if (!transfer_result.has_value()) {
        util::SlackAlerter::FundsAlerter().send("Failed to transfer and to write to clickhouse");
        return tl::make_unexpected("Failed to transfer and to write to clickhouse");
      }
      return result;
    }

    result = createNewLoansRow(to_subaccount, asset, transfer_amount, loan_info.initial_account, loan_info.loan_id);

    if (!result.has_value()) {
      auto transfer_result = transfer_command->undo();
      if (!transfer_result.has_value()) {
         util::SlackAlerter::FundsAlerter().send("Failed to transfer and to write to clickhouse");
        return tl::make_unexpected("Failed to transfer and to write to clickhouse");
      }
      return result;
    }

    amount -= transfer_amount;
    if (amount == 0) {
      break;
    }
  }
  return {};
}

tl::expected<void, std::string> LoansManager::deleteRowByLoanId(const std::string& table_name, const std::string& loan_id) {
  std::string query =
      std::format("ALTER TABLE {} UPDATE status = '{}' WHERE loan_id = '{}'", table_name, kRemoveLoanStatus, id);
  LOG_DEBUG("{}", query);
  try {
    clickhouse_client_->Execute({std::move(query)});
    query = std::format("ALTER TABLE {} DELETE WHERE loan_id = '{}'", table_name, id);
    LOG_DEBUG("{}", query);
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to delete row. Exception: "} + e.what());
  }
  return {};
}

tl::expected<void, std::string> LoansManager::deleteRowById(const std::string& table_name, const std::string& id) {
  std::string query =
      std::format("ALTER TABLE {} UPDATE status = '{}' WHERE id = '{}'", table_name, kRemoveLoanStatus, id);
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

tl::expected<void, std::string> LoansManager::changeAmountInRowById(const std::string& table_name,
                                                                    const std::string& id,
                                                                    infra::Volume new_amount) {
  std::string query = std::format("ALTER TABLE {} UPDATE amount = '{}' WHERE id = '{}'",
                                  table_name,
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

tl::expected<void, std::string> LoansManager::createNewBorrowRow(const std::string& subaccount,
                                                                 const std::string& asset,
                                                                 infra::Volume amount,
                                                                 const std::string& loan_id,
                                                                 infra::Exchange exchange) {
  infra::Volume amount_usd = amount * transfer::CryptoTransfer({exchange}).getLastPrice(asset, exchange);
  std::string query = std::format(
      "INSERT INTO {} (open_timestamp, subaccount, asset, amount, open_amount_usd, loan_id, status) VALUES ('{}', "
      "'{}', '{}', '{}', '{}', '{}', '{}')",
      kBorrowsTable,
      static_cast<int64_t>(nowSystem()) / 1'000'000,
      subaccount,
      asset,
      util::lexical_cast<std::string>(amount),
      util::lexical_cast<std::string>(amount_usd),
      loan_id,
      kDoneLoanStatus);
  try {
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to create new borrow row. Exception: "} + e.what());
  }
  return {};
}

tl::expected<void, std::string> LoansManager::createNewLoansRow(const std::string& subaccount,
                                                                const std::string& asset,
                                                                infra::Volume amount,
                                                                const std::string& initial_subaccount,
                                                                const std::string& loan_id) {
  std::string query = std::format(
      "INSERT INTO {} (timestamp, subaccount, asset, amount, initial_subaccount, type, loan_id, status) VALUES "
      "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
      kLoansInfoTable,
      static_cast<int64_t>(nowSystem()) / 1'000'000,
      subaccount,
      asset,
      util::lexical_cast<std::string>(amount),
      initial_subaccount,
      magic_enum::enum_name(LoanType::Normal),
      loan_id,
      kDoneLoanStatus);
  try {
    clickhouse_client_->Execute({std::move(query)});
  } catch (const std::exception& e) {
    LOG_ERROR("clickhouse error: {}", e.what());
    return tl::make_unexpected(std::string{"Failed to create new loans row. Exception: "} + e.what());
  }
  return {};
}


}  // namespace funds_controller
