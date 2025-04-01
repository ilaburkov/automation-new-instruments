#include "prod/funds_controller/main_commands.h"

#include "prod/transfer/transfer.h"

#include "util/assert/assert.h"
#include "util/error/error.h"

namespace funds_controller {

MergeCommands::MergeCommands(std::vector<std::unique_ptr<ICommand>> commands): commands_(std::move(commands)) {
}

tl::expected<void, std::string> MergeCommands::execute() {
  for (auto& command : commands_) {
    auto result = command->execute();
    EXPECT_WITH_STRING(result.has_value(), "Failed to execute command");
    ++executed_commands_count_;
  }
  return {};
}

tl::expected<void, std::string> MergeCommands::undo() {
  for (size_t i = executed_commands_count_; i >= 0; --i) {
    auto result = commands_[i]->undo();
    if (!result.has_value()) {
      // TODO: alert to slack
      EXPECT_WITH_STRING(false, "Failed to undo command");
    }
    ASSERT_FATAL(executed_commands_count_ > 0, "executed_commands_count_ should be always positive");
    --executed_commands_count_;
  }
  return {};
}

SendMarketCommand::SendMarketCommand(const std::string& subaccount,
                                     const infra::InstrumentDescription& instrument_description,
                                     infra::Volume amount):
    subaccount_(subaccount), instrument_description_(instrument_description), amount_(amount) {
}

tl::expected<void, std::string> SendMarketCommand::execute() {
  return transfer::CryptoTransfer({instrument_description_.value.market.exchange()})
      .sendMarket(subaccount_,
                  instrument_description_,
                  amount_ > 0 ? infra::Side::Bid : infra::Side::Ask,
                  util::decimal::abs(amount_));
}
tl::expected<void, std::string> SendMarketCommand::undo() {
  return transfer::CryptoTransfer({instrument_description_.value.market.exchange()})
      .sendMarket(subaccount_,
                  instrument_description_,
                  amount_ > 0 ? infra::Side::Ask : infra::Side::Bid,
                  util::decimal::abs(amount_));
}

TransferCryptoCommand::TransferCryptoCommand(const std::string& from_subaccount,
                                             infra::Wallet from_wallet,
                                             const std::string& to_subaccount,
                                             infra::Wallet to_wallet,
                                             const std::string& asset,
                                             infra::Volume amount):
    from_subaccount_(from_subaccount),
    from_wallet_(from_wallet),
    to_subaccount_(to_subaccount),
    to_wallet_(to_wallet),
    asset_(asset),
    amount_(amount) {
}

tl::expected<void, std::string> TransferCryptoCommand::execute() {
  return transfer::CryptoTransfer({from_wallet_.exchange()})
      .transfer(from_subaccount_, from_wallet_, to_subaccount_, to_wallet_, asset_, amount_);
}

tl::expected<void, std::string> TransferCryptoCommand::undo() {
  return transfer::CryptoTransfer({from_wallet_.exchange()})
      .transfer(to_subaccount_, to_wallet_, from_subaccount_, from_wallet_, asset_, amount_);
}

}  // namespace funds_controller
