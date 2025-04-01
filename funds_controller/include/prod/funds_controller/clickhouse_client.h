#pragma once

#include "util/decimal/decimal.h"
#include "util/env/env.h"

#include <clickhouse/client.h>

#include <memory>

namespace funds_controller {

std::unique_ptr<clickhouse::Client> getFundsControllerClickhouseClient();

std::string convertUUIDToString(const clickhouse::UUID& uuid);

util::Decimal convertClickhouseDecimalToDecimal(const clickhouse::Int128& decimal);

}  // namespace funds_controller
