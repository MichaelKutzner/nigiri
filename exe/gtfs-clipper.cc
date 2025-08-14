#include <filesystem>
#include <iostream>
#include <string_view>

#include "boost/json.hpp"
#include "boost/program_options.hpp"

#include "miniz.h"

#include "utl/read_file.h"
#include "utl/to_vec.h"

#include "geo/polygon.h"

#include "fmt/base.h"
#include "fmt/ostream.h"

#include "nigiri/loader/dir.h"
#include "nigiri/preprocessing/clipping/gtfs.h"

namespace fs = std::filesystem;
namespace bpo = boost::program_options;
using namespace std::literals::string_view_literals;
using namespace nigiri;

geo::simple_polygon as_polygon(boost::json::array const& a) {
  auto const to_latlng = [](boost::json::array const& x) -> geo::latlng {
    return {x.at(1).as_double(), x.at(0).as_double()};
  };
  return utl::to_vec(a, [&](auto&& y) { return to_latlng(y.as_array()); });
}

std::optional<geo::simple_polygon> read_geometry(fs::path const& p) {
  auto const content = utl::read_file(p.c_str());
  if (!content) {
    fmt::println(std::cerr, "Failed to read '{}'", p.c_str());
    return std::nullopt;
  }
  auto const pol = boost::json::parse(*content);
  auto const geometry = pol.try_as_object()
                            ->try_at("features")
                            ->try_as_array()
                            ->try_at(0U)
                            ->try_as_object()
                            ->try_at("geometry")
                            ->try_as_object();
  if (geometry.has_error()) {
    fmt::println(std::cerr, "Cannot find geometry");
    return std::nullopt;
  }
  auto const type = geometry->try_at("type")->try_as_string();
  if (type.has_error()) {
    fmt::println(std::cerr, "Cannot find 'type'");
    return std::nullopt;
  }
  if (type->data() != "Polygon"sv) {
    fmt::println(std::cerr, "Unsupported type '{}'", type->data());
    return std::nullopt;
  }
  auto const coords = geometry->try_at("coordinates")->try_as_array();
  if (coords.has_error()) {
    fmt::println(std::cerr, "Failed to get coordinates");
    return std::nullopt;
  }
  auto const ring0 = coords->try_at(0U)->try_as_array();
  if (ring0.has_error()) {
    fmt::println(std::cerr, "Missing ring 0 for cooordinates");
    return std::nullopt;
  }
  return as_polygon(*ring0);
}

int main(int ac, char** av) {
  auto in = fs::path{};
  auto out = fs::path{"gtfs-clipped.zip"};
  auto geometry = fs::path{"geometry.geojson"};

  auto desc = bpo::options_description{"Options"};
  desc.add_options()  //
      ("help,h", "produce this help message")  //
      ("in,i", bpo::value(&in), "input GTFS path")  //
      ("out,o", bpo::value(&out)->default_value(out), "output GTFS path")  //
      ("geometry,g", bpo::value(&geometry)->default_value(geometry),
       "geometry path")  //
      ("force,f", "force override of out GTFS file if exists");  //
  auto const pos = bpo::positional_options_description{}.add("in", -1);

  auto vm = bpo::variables_map{};
  bpo::store(
      bpo::command_line_parser(ac, av).options(desc).positional(pos).run(), vm);
  bpo::notify(vm);

  if (vm.count("help") != 0U) {
    std::cout << desc << "\n";
    return 0;
  }
  auto const force = vm.count("force") > 0U;
  if (in.empty() || !std::filesystem::is_regular_file(in)) {
    fmt::println(std::cerr, "Missing in GTFS feed '{}'", in.c_str());
    return 1;
  }
  if (exists(out) && !force) {
    fmt::println(std::cerr, "out GTFS feed '{}' already exists", out.c_str());
    return 1;
  }
  if (force && exists(out)) {
    if (is_directory(out)) {
      fmt::println(std::cerr, "out GTFS feed '{}' exists and is a directory",
                   out.c_str());
      return 1;
    }
    fmt::println(std::cerr, "Deleting already existing out file '{}' ...",
                 out.c_str());
    fs::remove(out);
  }
  if (!exists(geometry)) {
    fmt::println(std::cerr, "Missing geometry file '{}'", geometry.c_str());
    return 1;
  }

  auto const dir = loader::make_dir(in);
  auto const polygon = read_geometry(geometry);
  if (!polygon) {
    fmt::println(std::cerr, "Failed to load polygon from '{}'",
                 geometry.c_str());
    return 1;
  }

  auto ar = mz_zip_archive{};
  mz_zip_writer_init_file(&ar, out.string().data(), 0);
  preprocessing::clipping::clip_feed(*dir, *polygon, ar);
  mz_zip_writer_finalize_archive(&ar);
  mz_zip_writer_end(&ar);

  return 0;
}
