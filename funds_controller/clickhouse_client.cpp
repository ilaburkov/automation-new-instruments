#include "prod/funds_controller/clickhouse_client.h"

#include "util/env/env.h"

#include <iomanip>
#include <sstream>

namespace funds_controller {

std::unique_ptr<clickhouse::Client> getFundsControllerClickhouseClient() {
  const auto user = util::getEnv("CLICKHOUSE_FUNDS_CONTROLLER_USER", "default_funds_controller");
  const auto password = util::getEnv("CLICKHOUSE_FUNDS_CONTROLLER_PASSWORD", "xp2pW14mw!fzd?q");
  return std::make_unique<clickhouse::Client>(clickhouse::ClientOptions()
                                                  .SetHost("ah4ojmnosb.ap-southeast-1.aws.clickhouse.cloud")
                                                  .SetPort(9440)
                                                  .SetUser(user)
                                                  .SetPassword(password)
                                                  .SetSendRetries(5)
                                                  .SetSSLOptions(clickhouse::ClientOptions::SSLOptions()));
}


std::string convertUUIDToString(const clickhouse::UUID& uuid) {
  std::stringstream ss;
  ss << std::hex << std::setfill('0') << std::setw(8) << (uuid.first >> 32) << "-" << std::setw(4)
     << ((uuid.first >> 16) & 0xFFFF) << "-" << std::setw(4) << (uuid.first & 0xFFFF) << "-" << std::setw(4)
     << (uuid.second >> 48) << "-" << std::setw(12) << (uuid.second & 0xFFFFFFFFFFFF);
  return ss.str();
}

util::Decimal convertClickhouseDecimalToDecimal(const clickhouse::Int128& decimal) {
  return util::Decimal::withMantissa(static_cast<util::Decimal::BaseType>(decimal));
}

}  // namespace funds_controller
