#include "nigiri/common/geojson.h"

#include <string_view>

#include "utl/to_vec.h"
#include "utl/verify.h"

using namespace std::literals::string_view_literals;

namespace nigiri::common {

geo::simple_polygon as_polygon(boost::json::array const& a) {
  auto const to_latlng = [](boost::json::array const& x) -> geo::latlng {
    return {x.at(1).as_double(), x.at(0).as_double()};
  };
  return utl::to_vec(a, [&](auto&& y) { return to_latlng(y.as_array()); });
}

std::vector<geo::simple_polygon> parse_features(boost::json::object const& o) {
  auto const geometry = o.try_at("features")
                            ->try_as_array()
                            ->try_at(0U)
                            ->try_as_object()
                            ->try_at("geometry")
                            ->try_as_object();
  utl::verify(!geometry.has_error(), "Cannot find geometry");
  auto const type = geometry->try_at("type")->try_as_string();
  utl::verify(!type.has_error(), "Cannot find 'type'");
  utl::verify(type->data() == "Polygon"sv, "Unsupported type '{}'",
              type->data());
  auto const coords = geometry->try_at("coordinates")->try_as_array();
  utl::verify(!coords.has_error(), "Failed to get coordinates");
  auto const ring0 = coords->try_at(0U)->try_as_array();
  utl::verify(!ring0.has_error(), "Missing ring 0 for cooordinates");
  return {as_polygon(*ring0)};
}

std::vector<geo::simple_polygon> parse_features(std::string_view s) {
  auto const json = boost::json::parse(s);
  auto const o = json.try_as_object();
  utl::verify(o.has_value(), "Failed to parse geojson");
  return parse_features(*o);
}

}  // namespace nigiri::common
