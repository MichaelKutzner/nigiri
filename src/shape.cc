#include "nigiri/shape.h"

#include "geo/polyline.h"

#include "nigiri/timetable.h"

namespace nigiri {

geo::polyline get_shape(trip_idx_t trip_idx,
                        timetable const& tt,
                        shape_vecvec_t const& shapes) {
  if (trip_idx == trip_idx_t::invalid()) {
    return {};
  }
  return (trip_idx < tt.trip_shape_indices_.size())
             ? get_shape(tt.trip_shape_indices_[trip_idx], shapes)
             : geo::polyline{};
}

geo::polyline get_shape(shape_idx_t const shape_idx,
                        shape_vecvec_t const& shapes) {
  if (shape_idx == shape_idx_t::invalid()) {
    return {};
  }
  auto const& bucket = shapes.at(shape_idx);
  return geo::polyline(bucket.begin(), bucket.end());
}

std::vector<geo::polyline> get_shape_segments(trip_idx_t trip_idx,
                        timetable const& tt,
                        geo::polyline const& stops,
                        shape_vecvec_t const& shapes) {
  auto const shape = get_shape(trip_idx, tt, shapes);
  auto split_points = geo::polyline(stops.begin() + 1, stops.begin() + static_cast<long>(stops.size()) - 1);
  // auto const split_points = std::ranges::subrange(stops.begin() + 1, stops.begin() + static_cast<long>(stops.size()) - 2);
  auto const splits = geo::split_polyline(shape, split_points);
  std::vector<geo::polyline> segments;
  segments.reserve(stops.size() - 1u);
  auto last = stops.at(0);
  auto last_offset = 0;
  for (auto const& pair : splits) {
    auto segment = geo::polyline{};
    segment.push_back(last);
    segment.insert(segment.end(), shape.begin() + last_offset + 1, shape.begin() + static_cast<long>(pair.second) + 1u);
    segment.push_back(pair.first);
    segments.push_back(segment);
    last = pair.first;
    last_offset = static_cast<decltype(last_offset)>(pair.second);
  }
  auto segment = geo::polyline{};
  segment.push_back(last);
  segment.insert(segment.end(), shape.begin() + last_offset + 1u, shape.begin() + static_cast<long>(shape.size()) - 1u);
  segment.push_back(*(stops.rbegin()));
  segments.push_back(segment);

  return segments;
}

}  // namespace nigiri