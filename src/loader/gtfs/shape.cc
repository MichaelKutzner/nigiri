#include "nigiri/loader/gtfs/shape.h"

#include <vector>

#include "cista/strong.h"

#include "geo/latlng.h"

#include "utl/parser/buf_reader.h"
#include "utl/parser/csv_range.h"
#include "utl/parser/line_range.h"
#include "utl/pipes/for_each.h"
#include "utl/progress_tracker.h"

#include "nigiri/common/cached_lookup.h"
#include "nigiri/common/sort_by.h"

namespace nigiri::loader::gtfs {

shape_loader_state parse_shapes(std::string_view const data,
                                shapes_storage& shapes_data) {
  struct shape_entry {
    utl::csv_col<utl::cstr, UTL_NAME("shape_id")> id_;
    utl::csv_col<double, UTL_NAME("shape_pt_lat")> lat_;
    utl::csv_col<double, UTL_NAME("shape_pt_lon")> lon_;
    utl::csv_col<unsigned, UTL_NAME("shape_pt_sequence")> seq_;
    utl::csv_col<double, UTL_NAME("shape_dist_traveled")> distance_;
  };
  auto& shapes = shapes_data.data_;
  auto const index_offset = static_cast<shape_idx_t>(shapes.size());
  auto states = shape_loader_state{
      .index_offset_ = index_offset,
  };

  auto points = std::vector<std::vector<geo::latlng>>{};
  auto point_seq = std::vector<std::vector<unsigned>>{};
  auto distances = std::vector<std::vector<double>>{};
  auto ordering_required = std::vector<bool>{};
  auto lookup = cached_lookup(states.id_map_);

  auto const progress_tracker = utl::get_active_progress_tracker();
  progress_tracker->status("Parse Shapes")
      .out_bounds(37.F, 38.F)
      .in_high(data.size());
  utl::line_range{utl::make_buf_reader(data, progress_tracker->update_fn())}  //
      | utl::csv<shape_entry>()  //
      | utl::for_each([&](shape_entry const entry) {
          auto const shape_idx = lookup(entry.id_->view(), [&] {
            auto const idx = static_cast<shape_idx_t>(
                points.size() + cista::to_idx(index_offset));
            points.push_back({});
            point_seq.push_back({});
            distances.push_back({});
            ordering_required.push_back(false);
            return idx;
          });
          auto const idx = cista::to_idx(shape_idx - index_offset);
          auto const seq = *entry.seq_;
          if (!point_seq[idx].empty() && point_seq[idx].back() > seq) {
            ordering_required[idx] = true;
          }
          points[idx].emplace_back(geo::latlng{*entry.lat_, *entry.lon_});
          point_seq[idx].emplace_back(std::move(seq));
          if (distances[idx].empty()) {
            if (*entry.distance_ != 0.0) {
              distances[idx].resize(points[idx].size());
              distances[idx].back() = *entry.distance_;
            }
          } else {
            distances[idx].emplace_back(*entry.distance_);
          }
        });

  for (auto idx = 0U; idx < points.size(); ++idx) {
    if (ordering_required[idx]) {
      std::tie(point_seq[idx], points[idx], distances[idx]) =
          sort_by(point_seq[idx], points[idx], distances[idx]);
    }
    shapes_data.data_.emplace_back(std::move(points[idx]));
    states.distances_.emplace_back(std::move(distances[idx]));
  }

  return states;
}

}  // namespace nigiri::loader::gtfs
