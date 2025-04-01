#pragma once

#include "icommand.h"

#include "common/instrument_description/instrument_description.h"
#include "common/types/volume.h"
#include "common/wallet/wallet.h"

#include <memory>
#include <string>
#include <vector>

namespace funds_controller {

class MergeCommands : public ICommand {
public:
  MergeCommands(std::vector<std::unique_ptr<ICommand>> commands);

  tl::expected<void, std::string> execute() override;

  tl::expected<void, std::string> undo() override;

private:
  std::vector<std::unique_ptr<ICommand>> commands_;
  size_t executed_commands_count_ = 0;
};

class SendMarketCommand : public ICommand {
public:
  SendMarketCommand(const std::string& subaccount,
                    const infra::InstrumentDescription& instrument_description,
                    infra::Volume amount);

  tl::expected<void, std::string> execute() override;
  tl::expected<void, std::string> undo() override;

private:
  std::string subaccount_;
  infra::InstrumentDescription instrument_description_;
  infra::Volume amount_;
};

class TransferCryptoCommand : public ICommand {
public:
  TransferCryptoCommand(const std::string& from_subaccount,
                        infra::Wallet from_wallet,
                        const std::string& to_subaccount,
                        infra::Wallet to_wallet,
                        const std::string& asset,
                        infra::Volume amount);

  tl::expected<void, std::string> execute() override;
  tl::expected<void, std::string> undo() override;

private:
  std::string from_subaccount_;
  infra::Wallet from_wallet_;
  std::string to_subaccount_;
  infra::Wallet to_wallet_;
  std::string asset_;
  infra::Volume amount_;
};

}  // namespace funds_controller
