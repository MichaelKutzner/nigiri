#pragma once

#include <cstddef>
#include <vector>

#include "utl/enumerate.h"
#include "utl/get_or_create.h"
#include "utl/helpers/algorithm.h"
#include "utl/verify.h"
#include "utl/zip.h"

#include "nigiri/for_each_meta.h"
#include "nigiri/routing/many_search_state.h"
#include "nigiri/routing/one_to_all.h"
#include "nigiri/routing/one_to_many.h"
#include "nigiri/routing/pareto_set.h"
#include "nigiri/routing/query.h"
#include "nigiri/routing/raptor/raptor.h"
#include "nigiri/routing/raptor/raptor_state.h"
#include "nigiri/rt/rt_timetable.h"
#include "nigiri/types.h"

namespace nigiri::routing {

constexpr auto const kVias = via_offset_t{0U};

inline bitvec to_dest(many_search_state const& state,
                      unsigned int const n_locations) {
  auto d = bitvec{};
  d.resize(n_locations);
  for (auto const& l : state.lookup_) {
    d.set(to_idx(l.first), true);
  }
  return d;
}

template <direction SearchDir>
// std::vector<duration_t> one_to_many(
std::vector<delta_t> one_to_many([[maybe_unused]] timetable const& tt,
                                 [[maybe_unused]] rt_timetable const* rtt,
                                 [[maybe_unused]] many_search_state&& ms_state,
                                 [[maybe_unused]] query const& q) {
  utl::verify(std::holds_alternative<unixtime_t>(q.start_time_),
              "Start-time must be a time point (unixtime_t)");
  utl::verify(q.via_stops_.empty(),
              "One-to-All search not supported with vias");
  auto const& start_time = std::get<unixtime_t>(q.start_time_);

  auto state = raptor_state{};

  auto is_dest = to_dest(ms_state, tt.n_locations());
  auto is_via = std::array<bitvec, kMaxVias>{};
  auto dist_to_dest = std::vector<std::uint16_t>{};
  auto lb = std::vector<std::uint16_t>(tt.n_locations(), 0U);
  auto const base = make_base(tt, start_time);
  auto const is_wheelchair = q.prf_idx_ == kWheelchairProfile;

  constexpr auto const Rt = false;  // TODO Test rtt == nullptr
  auto algo = raptor<SearchDir, Rt, kVias, search_mode::kOneToMany>{
      tt,
      rtt,
      state,
      is_dest,
      is_via,
      dist_to_dest,
      q.td_dest_,
      lb,
      q.via_stops_,
      base,
      q.allowed_claszes_,
      q.require_bike_transport_,
      q.require_car_transport_,
      is_wheelchair,
      q.transfer_time_settings_,
      &ms_state};

  auto results = pareto_set<journey>{};
  algo.next_start_time();
  for (auto const& s : q.start_) {
    auto const t = SearchDir == direction::kForward ? start_time + s.duration()
                                                    : start_time - s.duration();
    trace("init: time_at_stop={} at {}\n", t, location_idx_t{s.target()});
    nigiri::routing::for_each_meta(
        tt, q.start_match_mode_, s.target(),
        [&](nigiri::location_idx_t const l) { algo.add_start(l, t); });
  }

  // Upper bound: Search journeys faster than 'worst_time_at_dest'
  // It will not find journeys with the same duration
  constexpr auto const kEpsilon = duration_t{1};
  auto const worst_time_at_dest =
      start_time +
      // TODO: Test if (sgn * (max) + 1) || (sgn * (max + 1)) ??
      (SearchDir == direction::kForward ? 1 : -1) * (q.max_travel_time_) +
      kEpsilon;

  algo.execute(start_time, q.max_transfers_, worst_time_at_dest, q.prf_idx_,
               results);

  return std::move(ms_state.best_);
}

}  // namespace nigiri::routing
