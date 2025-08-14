// Clip GTFS feed with a valid GeoJSON shape (Polygon / MultiPolygon)
//
// TODO
// - Clip with stop, location group or location
// - Template to copy tables when id contained in colletion
//

#include "nigiri/preprocessing/clipping/gtfs.h"

#include <iostream>

#include "utl/parser/cstr.h"
#include "utl/parser/csv_range.h"
#include "utl/parser/line_range.h"
#include "utl/pipes/for_each.h"

#include "nigiri/loader/gtfs/files.h"
#include "nigiri/logging.h"
#include "nigiri/types.h"

namespace nigiri::preprocessing::clipping {

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
  log(log_lvl::info, "get_stops_within", "Gathering stops within geometry");
  auto stops = hash_set<std::string>{};
  struct csv_stop {
    utl::csv_col<utl::cstr, UTL_NAME("stop_id")> id_;
    utl::csv_col<utl::cstr, UTL_NAME("stop_lat")> lat_;
    utl::csv_col<utl::cstr, UTL_NAME("stop_lon")> lon_;
  };

  utl::line_range{utl::make_buf_reader(content)}  //
      | utl::csv<csv_stop>()  //
      | utl::for_each([&](csv_stop const& s) {
          auto const x = geo::latlng{
              std::clamp(utl::parse<double>(s.lat_->trim()), -90.0, 90.0),
              std::clamp(utl::parse<double>(s.lon_->trim()), -180.0, 180.0)};
          if (geo::within(x, p)) {
            stops.insert(s.id_->to_str());
          }
        });
  return stops;
}

auto clip_trips(std::string_view const& content,
                hash_set<std::string> const& stops) {
  log(log_lvl::info, "clip_trips",
      "Gathering trips with stops within geometry");
  auto trips = hash_set<std::string>{};
  struct csv_stop_time {
    utl::csv_col<utl::cstr, UTL_NAME("trip_id")> trip_id_;
    utl::csv_col<utl::cstr, UTL_NAME("stop_id")> stop_id_;
  };

  utl::line_range{utl::make_buf_reader(content)}  //
      | utl::csv<csv_stop_time>()  //
      | utl::for_each([&](csv_stop_time& s) {
          if (stops.contains(s.stop_id_->view())) {
            trips.insert(s.trip_id_->to_str());
          }
        });
  return trips;
}

auto copy_stop_times(std::string_view const& content,
                     std::stringstream& ss,
                     hash_set<std::string> const& trips,
                     hash_set<std::string> const&& inner_stops) {
  log(log_lvl::info, "copy_stop_times", "Copying file '{}'",
      loader::gtfs::kStopTimesFile);
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
    log(log_lvl::info, "csv_stops", "Searching for parent stops");
    changed = false;
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
  log(log_lvl::info, "csv_stops", "Copying file '{}'", loader::gtfs::kStopFile);
  csv_copy<csv_stop>(ss, content, [&](csv_stop const& s) {
    return stops.contains(s.stop_id_->view());
  });
  return stops;
}

auto copy_trips(std::string_view const& content,
                std::stringstream& ss,
                hash_set<std::string> const& trips) {
  log(log_lvl::info, "csv_trips", "Copying file '{}'",
      loader::gtfs::kTripsFile);
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
  log(log_lvl::info, "csv_routes", "Copying file '{}'",
      loader::gtfs::kRoutesFile);
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
  log(log_lvl::info, "csv_agencies", "Copying file '{}'",
      loader::gtfs::kAgencyFile);
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
                   hash_set<std::string> const& services,
                   std::string_view const& filename) {
  log(log_lvl::info, "csv_calendar", "Copying file '{}'", filename);
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
  log(log_lvl::info, "csv_shapes", "Copying file '{}'",
      loader::gtfs::kShapesFile);
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
  log(log_lvl::info, "csv_transfers", "Copying file '{}'",
      loader::gtfs::kTransfersFile);
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
  log(log_lvl::info, "csv_frequencies", "Copying file '{}'",
      loader::gtfs::kFrequenciesFile);
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
  }
  auto ss = std::stringstream{};
  auto const add = [&](auto const& p) {
    auto const v = ss.view();
    mz_zip_writer_add_mem(&ar, p.data(), v.data(), v.size(),
                          MZ_BEST_COMPRESSION);
    source_files.erase(p);
    ss.str("");
  };
  auto const copy_file = [&](auto const& p) {
    log(log_lvl::info, "copy_file", "Copying file '{}'", p);
    auto const f = dir.get_file(p);
    auto const v = f.data();
    mz_zip_writer_add_mem(&ar, p.data(), v.data(), v.size(),
                          MZ_BEST_COMPRESSION);
    source_files.erase(p);
  };
  auto const inner_stops =
      get_stops_within(dir.get_file(loader::gtfs::kStopFile).data(), polygon);
  log(log_lvl::debug, "clip-feed", "Number of contained stops: {}",
      inner_stops.size());
  auto const clipped_trips = clip_trips(
      dir.get_file(loader::gtfs::kStopTimesFile).data(), inner_stops);
  log(log_lvl::debug, "clip-feed", "Number of contained trips: {}",
      clipped_trips.size());
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
                  services, loader::gtfs::kCalenderFile);
    add(loader::gtfs::kCalenderFile);
  }
  // calendar_dates.txt (optional)
  if (source_files.contains(loader::gtfs::kCalendarDatesFile)) {
    copy_calendar(dir.get_file(loader::gtfs::kCalendarDatesFile).data(), ss,
                  services, loader::gtfs::kCalendarDatesFile);
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
  log(log_lvl::debug, "clip-feed", "Unhandled files: {}", source_files);
  for (auto const& s : source_files) {
    if (s.empty()) {
      continue;
    }
    copy_file(s);
  }
}

}  // namespace nigiri::preprocessing::clipping
