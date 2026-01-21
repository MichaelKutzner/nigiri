#pragma once

#include "nigiri/routing/raptor/raptor.h"
#include "nigiri/types.h"

namespace nigiri::routing {

template <direction SearchDir,
          bool Rt,
          via_offset_t Vias,
          search_mode SearchMode>
void run_raptor(raptor<SearchDir, Rt, Vias, SearchMode>&& algo,
                timetable const& tt,
                unixtime_t const& start_time,
                query const& q) {
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
}

}  // namespace nigiri::routing
