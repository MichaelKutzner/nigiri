#include "nigiri/loader/gtfs/shape_prepare.h"

#include <chrono>
#include <algorithm>
#include <ranges>
#include <span>
#include <type_traits>

#include "geo/latlng.h"
#include "geo/polyline.h"

#include "utl/enumerate.h"
#include "utl/get_or_create.h"
#include "utl/progress_tracker.h"

#include "nigiri/stop.h"
#include "nigiri/types.h"

namespace nigiri::loader::gtfs {

std::size_t get_closest(geo::latlng const& pos,
                        std::span<geo::latlng const> shape) {
  if (shape.size() < 2U) {
    return 0U;
  }
  auto const best = geo::distance_to_polyline(pos, shape);
  auto const from = shape[best.segment_idx_];
  auto const to = shape[best.segment_idx_ + 1];
  return geo::distance(pos, from) <= geo::distance(pos, to)
             ? best.segment_idx_
             : best.segment_idx_ + 1;
}
std::pair<std::size_t, double> get_closest2(geo::latlng const& pos,
                        std::span<geo::latlng const> shape) {
  if (shape.size() < 2U) {
    return std::pair{0U, 0.0};
  }
  auto const best = geo::distance_to_polyline(pos, shape);
  auto const from = shape[best.segment_idx_];
  auto const to = shape[best.segment_idx_ + 1];
  return geo::distance(pos, from) <= geo::distance(pos, to)
             ? std::pair{best.segment_idx_, best.distance_to_polyline_}
             : std::pair{best.segment_idx_ + 1, best.distance_to_polyline_};
}

std::vector<shape_offset_t> get_offsets_by_stops(
    timetable const& tt,
    std::span<geo::latlng const> shape,
    stop_seq_t const& stop_seq) {
  // Need at least 1 shape point per stop
  if (shape.size() < stop_seq.size()) {
    return {};
  }

  auto offsets = std::vector<shape_offset_t>(stop_seq.size());
  auto remaining_start= cista::base_t<shape_offset_t>{1U};
  // Reserve space to map each stop to a different point
  auto max_width = shape.size() - stop_seq.size();

  for (auto const [i, s] : utl::enumerate(stop_seq)) {
    if (i == 0U) {
      offsets[0] = shape_offset_t{0U};
    } else if (i == stop_seq.size() - 1U) {
      offsets[i] = shape_offset_t{shape.size() - 1U};
    } else {
      auto const pos = tt.locations_.coordinates_[stop{s}.location_idx()];
      auto const offset =
          get_closest(pos, shape.subspan(remaining_start, max_width + 1U));
      offsets[i] = shape_offset_t{remaining_start + offset};
      remaining_start += offset + 1U;
      max_width -= offset;
    }
  }

  return offsets;
}

struct offset_pair {
  offset_pair next() const {
    return {
      stop_ + 1U,
      shape_ + 1U
    };
  }
  // std::size_t stop_;
  unsigned stop_;
  shape_offset_t shape_;
};

struct best_fit {
  double distance_{0.0};
  shape_offset_t best_{shape_offset_t{0U}};
  // offset_pair best_{0U, shape_offset_t{0U}};
};

void match_best_fit(auto& fits,
    timetable const& tt,
    std::span<geo::latlng const> shape,
    stop_seq_t const& stop_seq,
    offset_pair const& from,
    offset_pair const& to
) {
  auto const segment_width = to.stop_ - from.stop_;
  if (segment_width < 2U) {
    return;
  }
  auto const width = static_cast<unsigned>((to.shape_ - from.shape_) - (to.stop_ - from.stop_) + 1U);
  auto const stop_offset = from.shape_ - from.stop_;
  auto min_dist = 0.0;
  auto min_pos = 0U;
  for (auto stop_index = from.stop_ +1; stop_index < to.stop_; ++stop_index) {
    auto& curr = fits[stop_index];
    auto const shape_offset = stop_index + stop_offset.v_;
    if (curr.best_ < shape_offset || curr.best_ >= shape_offset + width) {
      auto const pos = tt.locations_.coordinates_[stop{stop_seq[stop_index]}.location_idx()];
      auto const [offset, dist] = get_closest2(pos, shape.subspan(shape_offset, width));
      curr.distance_ = dist;
      curr.best_ = static_cast<shape_offset_t>(shape_offset + offset);
    }
    if (min_pos == 0 || min_dist > curr.distance_) {
      min_dist = curr.distance_;
      min_pos = stop_index;
    }
  }
  auto const split_point = offset_pair{min_pos, fits[min_pos].best_};
  match_best_fit(fits, tt, shape, stop_seq, from, split_point);
  match_best_fit(fits, tt, shape, stop_seq, split_point, to);

}

std::vector<shape_offset_t> get_offsets_by_best_fit_stops(
    timetable const& tt,
    std::span<geo::latlng const> shape,
    stop_seq_t const& stop_seq) {
  auto best_fits = std::vector<best_fit>(stop_seq.size());
  // best_fits.back() = best_fit{0.0, shape_offset_t{shape.size() - 1U}};
  best_fits.back() = best_fit{0.0, shape_offset_t{shape.size() - 1U}};

  // match_best_fit(best_fits, tt, shape, {0U, shape_offset_t{1U}}, {static_cast<unsigned>(stop_seq.size() - 2U), shape_offset_t{stop_seq.size() - 2U}});
  // match_best_fit(best_fits, tt, shape, {0U, shape_offset_t{1U}}, {static_cast<unsigned>(stop_seq.size() - 1U), shape_offset_t{stop_seq.size() - 1U}});
  match_best_fit(best_fits, tt, shape, stop_seq, {0U, shape_offset_t{0U}}, {static_cast<unsigned>(stop_seq.size() - 1U), shape_offset_t{shape.size() - 1U}});

  auto offsets = std::vector<shape_offset_t>(stop_seq.size());
  for (auto [best, offset]: utl::zip(best_fits, offsets)) {
    offset = best.best_;
  }

  return offsets;
}

template <typename DoubleRange>
  requires std::ranges::range<DoubleRange> &&
           std::is_same_v<std::ranges::range_value_t<DoubleRange>, double>
std::vector<shape_offset_t> get_offsets_by_dist_traveled(
    std::vector<double> const& dist_traveled_stops_times,
    DoubleRange const& dist_traveled_shape_edges) {
  auto offsets = std::vector<shape_offset_t>{};
  offsets.reserve(dist_traveled_stops_times.size());
  auto remaining_shape_begin = begin(dist_traveled_shape_edges);
  for (auto const& distance : dist_traveled_stops_times) {
    remaining_shape_begin = std::lower_bound(
        remaining_shape_begin, end(dist_traveled_shape_edges), distance);
    offsets.push_back(shape_offset_t{remaining_shape_begin -
                                     begin(dist_traveled_shape_edges)});
  }
  return offsets;
}

void calculate_shape_offsets(timetable const& tt,
                             shapes_storage& shapes_data,
                             vector_map<gtfs_trip_idx_t, trip> const& trips,
                             shape_loader_state const& shape_states) {
  auto const start = std::chrono::high_resolution_clock::now();
  auto const progress_tracker = utl::get_active_progress_tracker();
  progress_tracker->status("Calculating shape offsets")
      .out_bounds(98.F, 100.F)
      .in_high(trips.size());

  auto const key_hash =
      [](std::pair<shape_idx_t, stop_seq_t const*> const& pair) noexcept {
        auto h = cista::BASE_HASH;
        h = cista::hash_combine(h, cista::hashing<shape_idx_t>{}(pair.first));
        h = cista::hash_combine(h, cista::hashing<stop_seq_t>{}(*pair.second));
        return h;
      };
  auto const key_compare =
      [](std::pair<shape_idx_t, stop_seq_t const*> const& lhs,
         std::pair<shape_idx_t, stop_seq_t const*> const& rhs) noexcept {
        return (lhs.first == rhs.first) && (*lhs.second == *rhs.second);
      };
  auto shape_offsets_cache =
      hash_map<std::pair<shape_idx_t, stop_seq_t const*>, shape_offset_idx_t,
               decltype(key_hash), decltype(key_compare)>{};
  for (auto const& trip : trips) {
    progress_tracker->increment();
    auto const trip_idx = trip.trip_idx_;
    auto const shape_idx = trip.shape_idx_;

    auto const shape_offset_idx = utl::get_or_create(
        shape_offsets_cache, std::pair{shape_idx, &trip.stop_seq_}, [&]() {
          if (shape_idx == shape_idx_t::invalid() ||
              trip.stop_seq_.size() < 2U) {
            return shape_offset_idx_t::invalid();
          }
          auto const& shape_distances =
              shape_states.distances_[shape_idx - shape_states.index_offset_];
          if (!shape_distances.empty() && !trip.distance_traveled_.empty()) {
            auto const offsets = get_offsets_by_dist_traveled(
                trip.distance_traveled_, shape_distances);
            return shapes_data.add_offsets(offsets);
          }
          auto const shape = shapes_data.get_shape(shape_idx);
          if (shape.size() < trip.stop_seq_.size()) {
            return shape_offset_idx_t::invalid();  // >= 1 shape/point required
          }
          auto const offsets = get_offsets_by_best_fit_stops(tt, shape, trip.stop_seq_);
          return shapes_data.add_offsets(offsets);
        });
    shapes_data.add_trip_shape_offsets(
        trip_idx, cista::pair{shape_idx, shape_offset_idx});
  }
  auto const end = std::chrono::high_resolution_clock::now();
  auto const duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "Calculation took " << duration << "ms\n";
}

}  // namespace nigiri::loader::gtfs