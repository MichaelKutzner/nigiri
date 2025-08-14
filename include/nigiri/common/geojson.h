#include <string_view>
#include <vector>

#include "boost/json.hpp"

#include "geo/polygon.h"

namespace nigiri::common {

std::vector<geo::simple_polygon> parse_features(boost::json::object const&);

std::vector<geo::simple_polygon> parse_features(std::string_view);

}  // namespace nigiri::common
