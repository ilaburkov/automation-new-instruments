#pragma once

#include <tl/expected.hpp>

#include <string>

namespace funds_controller {

class ICommand {
public:
  virtual tl::expected<void, std::string> execute() = 0;
  virtual tl::expected<void, std::string> undo() = 0;
  virtual ~ICommand() = default;
};

}  // namespace funds_controller
