#include "nigiri/loader/gtfs/shape.h"

#include <algorithm>

#include "geo/latlng.h"

#include "utl/parser/buf_reader.h"
#include "utl/parser/csv_range.h"
#include "utl/parser/line_range.h"
#include "utl/pipes/for_each.h"
#include "utl/progress_tracker.h"

#include "nigiri/common/cached_lookup.h"
#include "nigiri/logging.h"

namespace nigiri::loader::gtfs {

shape_loader_state parse_shapes(std::string_view const data,
                                shapes_storage& shapes_data) {
  auto& shapes = shapes_data.data_;
  struct shape_entry {
    utl::csv_col<utl::cstr, UTL_NAME("shape_id")> id_;
    utl::csv_col<double, UTL_NAME("shape_pt_lat")> lat_;
    utl::csv_col<double, UTL_NAME("shape_pt_lon")> lon_;
    utl::csv_col<std::size_t, UTL_NAME("shape_pt_sequence")> seq_;
    utl::csv_col<double, UTL_NAME("shape_dist_traveled")> distance_;
  };

  auto const index_offset = static_cast<shape_idx_t>(shapes.size());
  auto states = shape_loader_state{
      .index_offset_ = index_offset,
  };
  auto lookup = cached_lookup(states.id_map_);

  auto const progress_tracker = utl::get_active_progress_tracker();
  progress_tracker->status("Parse Shapes")
      .out_bounds(37.F, 38.F)
      .in_high(data.size());
  utl::line_range{utl::make_buf_reader(data, progress_tracker->update_fn())}  //
      | utl::csv<shape_entry>()  //
      |
      utl::for_each([&](shape_entry const entry) {
        auto& state = lookup(entry.id_->view(), [&] {
          auto const index = static_cast<shape_idx_t>(shapes.size());
          shapes.add_back_sized(0U);
          states.distances_.push_back({});
          return shape_state{index, 0U, 0.0};
        });
        auto const seq = *entry.seq_;
        auto const distance_traveled = *entry.distance_;
        auto bucket = shapes[state.index_];
        if (!bucket.empty()) {
          if (state.last_seq_ >= seq) {
            log(log_lvl::error, "loader.gtfs.shape",
                "Non monotonic sequence for shape_id '{}': Sequence number {} "
                "followed by {}",
                entry.id_->to_str(), state.last_seq_, seq);
          }
          // Store average to allow small rounding errors
          states.distances_[state.index_ - index_offset].push_back(
              (state.last_distance_traveled_ + distance_traveled) / 2.0);
        }
        bucket.push_back(geo::latlng{*entry.lat_, *entry.lon_});
        state.last_seq_ = seq;
        state.last_distance_traveled_ = distance_traveled;
      });
  for (auto distances : states.distances_) {
    if (!std::ranges::any_of(
            distances, [](double const distance) { return distance > 0.0; })) {
      distances.clear();
    }
  }
  return states;
}

}  // namespace nigiri::loader::gtfs
