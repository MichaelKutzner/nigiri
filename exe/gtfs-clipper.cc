// Clip GTFS feed with a valid GeoJSON shape (Polygon / MultiPolygon)
//
// TODO
// - Clip with stop, location group or location
// - Template to copy tables when id contained in colletion
//

#include <filesystem>
#include <iostream>
#include <ranges>
#include <string_view>

// #include "boost/geometry/geometry.hpp"

#include "utl/parser/cstr.h"
#include "utl/parser/csv_range.h"
#include "utl/parser/line_range.h"
#include "utl/pipes/for_each.h"
#include "utl/read_file.h"
#include "utl/to_vec.h"
// #include "utl/pipes/transform.h"
// #include "utl/pipes/vec.h"

#include "boost/geometry/geometry.hpp"
#include "boost/json.hpp"
#include "boost/program_options.hpp"

#include "miniz.h"

#include "geo/polygon.h"

#include "fmt/base.h"

#include "nigiri/loader/dir.h"
#include "nigiri/loader/gtfs/files.h"
#include "nigiri/logging.h"
#include "nigiri/types.h"

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
  fmt::println("Content: '{}'", *content);
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

template <typename T>
void csv_copy(std::ostream& out,
              std::string_view const& content,
              std::function<bool(T const&)> const& filter) {
  auto reader = utl::line_range{utl::make_buf_reader(content)};
  auto it = reader.begin();
  // Always copy header row
  out << it.view() << '\n';
  utl::for_each_row<T>(content, [&](auto const& row) {
    reader.next(it);
    if (filter(row)) {
      out << it.view() << '\n';
    }
  });
}

auto get_stops_within(std::string_view const& content,
                      geo::simple_polygon const& p) {
  auto stops = hash_set<std::string>{};
  struct csv_stop {
    utl::csv_col<utl::cstr, UTL_NAME("stop_id")> id_;
    utl::csv_col<utl::cstr, UTL_NAME("stop_lat")> lat_;
    utl::csv_col<utl::cstr, UTL_NAME("stop_lon")> lon_;
  };

  utl::line_range{utl::make_buf_reader(content/*,
                                       progress_tracker->update_fn()*/)}  //
      | utl::csv<csv_stop>()  //
      |
      utl::for_each([&](csv_stop const& s) {
    auto const x = geo::latlng{
        std::clamp(utl::parse<double>(s.lat_->trim()), -90.0, 90.0),
        std::clamp(utl::parse<double>(s.lon_->trim()), -180.0, 180.0)
    };
    if (geo::within(x, p)) {
        stops.insert(s.id_->to_str());
        // fmt::println("Polygon {} contains {}", p, x);
    }
});
  // ---- TEST START
  csv_copy<csv_stop>(std::cout, content, [&](csv_stop const& s) {
    return stops.contains(s.id_->view());
  });
  // ---- TEST END
  return stops;
}

auto clip_trips(std::string_view const& content,
                hash_set<std::string> const& stops) {
  auto trips = hash_set<std::string>{};
  struct csv_stop_time {
    utl::csv_col<utl::cstr, UTL_NAME("trip_id")> trip_id_;
    utl::csv_col<utl::cstr, UTL_NAME("stop_id")> stop_id_;
  };

  utl::line_range{utl::make_buf_reader(content/*,
                                       progress_tracker->update_fn()*/)}  //
      | utl::csv<csv_stop_time>()  //
      |
      utl::for_each([&](csv_stop_time& s) {
      if (stops.contains(s.stop_id_->view())) {
        trips.insert(s.trip_id_->to_str());
    }
});
  return trips;
}

auto const copy_stop_times(std::string_view const& content,
                           std::stringstream& ss,
                           hash_set<std::string> const& trips,
                           hash_set<std::string> const&& inner_stops) {
  auto stops = std::move(inner_stops);
  struct csv_stop_time {
    utl::csv_col<utl::cstr, UTL_NAME("trip_id")> trip_id_;
    utl::csv_col<utl::cstr, UTL_NAME("stop_id")> stop_id_;
    // utl::csv_col<utl::cstr, UTL_NAME("location_group_id")>
    // location_group_id_; utl::csv_col<utl::cstr, UTL_NAME("location_id")>
    // location_id_;
  };

  csv_copy<csv_stop_time>(ss, content, [&](csv_stop_time const& s) {
    if (trips.contains(s.trip_id_->view())) {
      stops.insert(s.stop_id_->view());
      return true;
    } else {
      return false;
    }
  });
  //   utl::line_range{utl::make_buf_reader(content/*,
  //                                        progress_tracker->update_fn()*/)} //
  //       | utl::csv<csv_stop_time>()  //
  //       |
  //       utl::for_each([&](csv_stop_time& s) {
  //       if (trips.contains(s.trip_id_->view())) {
  //         stops.insert(s.stop_id_->to_str());
  //     }
  // });
  return stops;
}

auto copy_stops(std::string_view const& content,
                std::stringstream& ss,
                hash_set<std::string> const& reachable_stops) {
  struct csv_stop {
    utl::csv_col<utl::cstr, UTL_NAME("stop_id")> stop_id_;
    utl::csv_col<utl::cstr, UTL_NAME("parent_station")> parent_station_;
  };
  auto stops = std::move(reachable_stops);
  auto changed = false;
  do {
    changed = false;
    std::cout << "Looping ...\n";
    utl::for_each_row<csv_stop>(content, [&](csv_stop const& s) {
      if (!stops.contains(s.stop_id_->view())) {
        return;
      }
      if (!s.parent_station_->empty() &&
          !stops.contains(s.parent_station_->view())) {
        stops.insert(s.parent_station_->view());
        changed = true;
      }
    });
  } while (changed);
  csv_copy<csv_stop>(ss, content, [&](csv_stop const& s) {
    return stops.contains(s.stop_id_->view());
  });
  return stops;
}

auto copy_trips(std::string_view const& content,
                std::stringstream& ss,
                hash_set<std::string> const& trips) {
  struct csv_trip {
    utl::csv_col<utl::cstr, UTL_NAME("trip_id")> trip_id_;
    utl::csv_col<utl::cstr, UTL_NAME("route_id")> route_id_;
    utl::csv_col<utl::cstr, UTL_NAME("service_id")> service_id_;
    utl::csv_col<utl::cstr, UTL_NAME("shape_id")> shape_id_;
  };
  auto routes = hash_set<std::string>{};
  auto services = hash_set<std::string>{};
  auto shapes = hash_set<std::string>{};
  csv_copy<csv_trip>(ss, content, [&](csv_trip const& t) {
    if (trips.contains(t.trip_id_->view())) {
      routes.insert(t.route_id_->view());
      services.insert(t.service_id_->view());
      if (!t.shape_id_->empty()) {
        shapes.insert(t.shape_id_->view());
      }
      return true;
    } else {
      return false;
    }
  });
  return std::tuple{routes, services, shapes};
}

auto copy_routes(std::string_view const& content,
                 std::stringstream& ss,
                 hash_set<std::string> const& routes) {
  struct csv_route {
    utl::csv_col<utl::cstr, UTL_NAME("route_id")> route_id_;
    utl::csv_col<utl::cstr, UTL_NAME("agency_id")> agency_id_;
  };
  auto agencies = hash_set<std::string>{};
  csv_copy<csv_route>(ss, content, [&](csv_route const& r) {
    if (routes.contains(r.route_id_->view())) {
      if (!r.agency_id_->empty()) {
        agencies.insert(r.agency_id_->view());
      }
      return true;
    } else {
      return false;
    }
  });
  return agencies;
}

auto copy_agencies(std::string_view const& content,
                   std::stringstream& ss,
                   hash_set<std::string> const& agencies) {
  struct csv_agency {
    utl::csv_col<utl::cstr, UTL_NAME("agency_id")> agency_id_;
  };
  csv_copy<csv_agency>(ss, content, [&](csv_agency const& a) {
    return a.agency_id_->empty() || agencies.size() == 0U ||
           agencies.contains(a.agency_id_->view());
  });
}

auto copy_calendar(std::string_view const& content,
                   std::stringstream& ss,
                   hash_set<std::string> const& services) {
  struct csv_calendar {
    utl::csv_col<utl::cstr, UTL_NAME("service_id")> service_id_;
  };
  csv_copy<csv_calendar>(ss, content, [&](csv_calendar const& c) {
    return services.contains(c.service_id_->view());
  });
}

auto copy_shapes(std::string_view const& content,
                 std::stringstream& ss,
                 hash_set<std::string> const& shapes) {
  struct csv_shape {
    utl::csv_col<utl::cstr, UTL_NAME("shape_id")> shape_id_;
  };
  csv_copy<csv_shape>(ss, content, [&](csv_shape const& s) {
    return shapes.contains(s.shape_id_->view());
  });
}

auto copy_transfers(std::string_view const& content,
                    std::stringstream& ss,
                    hash_set<std::string> const& stops,
                    hash_set<std::string> const& routes,
                    hash_set<std::string> const& trips) {
  struct csv_transfer {
    utl::csv_col<utl::cstr, UTL_NAME("from_stop_id")> from_stop_id_;
    utl::csv_col<utl::cstr, UTL_NAME("to_stop_id")> to_stop_id_;
    utl::csv_col<utl::cstr, UTL_NAME("from_route_id")> from_route_id_;
    utl::csv_col<utl::cstr, UTL_NAME("to_route_id")> to_route_id_;
    utl::csv_col<utl::cstr, UTL_NAME("from_trip_id")> from_trip_id_;
    utl::csv_col<utl::cstr, UTL_NAME("to_trip_id")> to_trip_id_;
  };
  csv_copy<csv_transfer>(ss, content, [&](csv_transfer const& t) {
    return (!t.from_stop_id_->empty() && !t.to_stop_id_->empty() &&
            stops.contains(t.from_stop_id_->view()) &&
            stops.contains(t.to_stop_id_->view())) ||
           (!t.from_route_id_->empty() && !t.to_route_id_->empty() &&
            routes.contains(t.from_route_id_->view()) &&
            routes.contains(t.to_route_id_->view())) ||
           (!t.from_trip_id_->empty() && !t.to_trip_id_->empty() &&
            trips.contains(t.from_trip_id_->view()) &&
            trips.contains(t.to_trip_id_->view()));
  });
}

auto copy_frequencies(std::string_view const& content,
                      std::stringstream& ss,
                      hash_set<std::string> const& trips) {
  struct csv_frequency {
    utl::csv_col<utl::cstr, UTL_NAME("trip_id")> trip_id_;
  };
  csv_copy<csv_frequency>(ss, content, [&](csv_frequency const& f) {
    return trips.contains(f.trip_id_->view());
  });
}

void clip_feed(nigiri::loader::dir const& dir,
               geo::simple_polygon const& polygon,
               mz_zip_archive& ar) {
  auto source_files = hash_set<std::string>{};
  for (auto const& p : dir.list_files(".")) {
    source_files.insert(p.string());
  };
  // auto source_files =
  //     dir.list_files(".")  //
  //     | std::views::transform([](auto const& p) { return p.string(); });
  auto ss = std::stringstream{};
  auto const add = [&](auto const& p) {
    auto const v = ss.view();
    mz_zip_writer_add_mem(&ar, p.data(), v.data(), v.size(),
                          MZ_BEST_COMPRESSION);
    source_files.erase(p);
    ss.str("");
  };
  auto const copy_file = [&](auto const& p) {
    auto const f = dir.get_file(p);
    auto const v = f.data();
    mz_zip_writer_add_mem(&ar, p.data(), v.data(), v.size(),
                          MZ_BEST_COMPRESSION);
    source_files.erase(p);
  };
  // auto get_content = [&](auto const& p) { return dir.get_file(p).data(); };
  auto const inner_stops =
      get_stops_within(dir.get_file(loader::gtfs::kStopFile).data(), polygon);
  fmt::println("Number of inner stops: {}", inner_stops.size());
  auto const clipped_trips = clip_trips(
      dir.get_file(loader::gtfs::kStopTimesFile).data(), inner_stops);
  fmt::println("Number of usable trips: {}", clipped_trips.size());
  // stop_times.txt
  auto const reachable_stops =
      copy_stop_times(dir.get_file(loader::gtfs::kStopTimesFile).data(), ss,
                      clipped_trips, std::move(inner_stops));
  add(loader::gtfs::kStopTimesFile);
  // stops.txt (optional)
  auto const all_stops =
      source_files.contains(loader::gtfs::kStopFile) ? [&]() {
        auto const stops =
            copy_stops(dir.get_file(loader::gtfs::kStopFile).data(), ss,
                       std::move(reachable_stops));
        add(loader::gtfs::kStopFile);
        return stops;
      }()
                                                     : hash_set<std::string>{};
  // if (source_files.contains(loader::gtfs::kStopFile)) {
  //   copy_stops(dir.get_file(loader::gtfs::kStopFile).data(), ss,
  //              std::move(reachable_stops));
  //   add(loader::gtfs::kStopFile);
  // }
  // trips.txt
  auto const [routes, services, shapes] = copy_trips(
      dir.get_file(loader::gtfs::kTripsFile).data(), ss, clipped_trips);
  add(loader::gtfs::kTripsFile);
  // routes.txt
  auto const agencies =
      copy_routes(dir.get_file(loader::gtfs::kRoutesFile).data(), ss, routes);
  add(loader::gtfs::kRoutesFile);
  // agency.txt
  copy_agencies(dir.get_file(loader::gtfs::kAgencyFile).data(), ss, agencies);
  add(loader::gtfs::kAgencyFile);
  // calendar.txt (optional)
  if (source_files.contains(loader::gtfs::kCalenderFile)) {
    copy_calendar(dir.get_file(loader::gtfs::kCalenderFile).data(), ss,
                  services);
    add(loader::gtfs::kCalenderFile);
  }
  // calendar_dates.txt (optional)
  if (source_files.contains(loader::gtfs::kCalendarDatesFile)) {
    copy_calendar(dir.get_file(loader::gtfs::kCalendarDatesFile).data(), ss,
                  services);
    add(loader::gtfs::kCalendarDatesFile);
  }
  // shapes.txt (optional)
  if (source_files.contains(loader::gtfs::kShapesFile)) {
    copy_shapes(dir.get_file(loader::gtfs::kShapesFile).data(), ss, shapes);
    add(loader::gtfs::kShapesFile);
  }
  // frequencies.txt (optional)
  if (source_files.contains(loader::gtfs::kFrequenciesFile)) {
    copy_frequencies(dir.get_file(loader::gtfs::kFrequenciesFile).data(), ss,
                     clipped_trips);
    add(loader::gtfs::kFrequenciesFile);
  }
  // transfers.txt (optional)
  if (source_files.contains(loader::gtfs::kTransfersFile)) {
    copy_transfers(dir.get_file(loader::gtfs::kTransfersFile).data(), ss,
                   all_stops, routes, clipped_trips);
    add(loader::gtfs::kTransfersFile);
  }
  // feed_info.txt
  copy_file(loader::gtfs::kFeedInfoFile);
  fmt::println("Number of reachable stops: {}", reachable_stops.size());
  fmt::println("Unhandled files: {}", source_files);
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
  auto const polygon = read_geometry(geometry);
  if (!polygon) {
    fmt::println(std::cerr, "Failed to load polygon from '{}'",
                 geometry.c_str());
    return 1;
  }
  fmt::println("Polygon: {}", *polygon);
  auto const dir = loader::make_dir(in);
  auto ar = mz_zip_archive{};
  mz_zip_writer_init_file(&ar, out.string().data(), 0);
  // fmt::println("Path: {}", dir->path().string());
  // for (auto const& x : dir->list_files(".")) {
  //   fmt::println("File: '{}'", x.filename().string());
  // }
  clip_feed(*dir, *polygon, ar);
  mz_zip_writer_finalize_archive(&ar);
  mz_zip_writer_end(&ar);
  // for (auto const& t : clipped_trips) {
  //   fmt::println("Added trip {}", t);
  // }
  // std::cout << "Hello, World!\n";
  return 0;
}
