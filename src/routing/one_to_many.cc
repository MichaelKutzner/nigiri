#include "nigiri/routing/one_to_many.h"

#include <cstddef>
#include <limits>
#include <vector>

#include "utl/enumerate.h"
#include "utl/get_or_create.h"
#include "utl/helpers/algorithm.h"
#include "utl/verify.h"
#include "utl/zip.h"

#include "date/date.h"

#include "nigiri/common/delta_t.h"
#include "nigiri/routing/many_search_state.h"
#include "nigiri/routing/one_to_all.h"
#include "nigiri/routing/query.h"
#include "nigiri/routing/raptor/run_raptor.h"
#include "nigiri/types.h"

namespace nigiri::routing {

// TODO Move struct + implementation
void update_worst(many_search_state& state) {
  auto idx = std::size_t{0U};
  state.worst_.delta_ = std::numeric_limits<delta_t>::min();
  for (auto const [offs, best] : utl::zip(state.dest_offsets_, state.best_)) {
    if (!offs.empty()) {  // Ignore all entries without reachable location
      if (best > state.worst_.delta_) {
        state.worst_.delta_ = best;
        state.worst_.offset_ = idx;
      }
    }
    ++idx;
  }
}

many_search_state::many_search_state(
    std::vector<std::vector<offset>> const offsets)
    : dest_offsets_{std::move(offsets)},
      best_{std::vector(dest_offsets_.size(),
                        std::numeric_limits<delta_t>::max())},
      worst_{} {
  for (auto const [idx, dest] : utl::enumerate(dest_offsets_)) {
    for (auto const& offset : dest) {
      utl::get_or_create(lookup_, offset.target(),
                         []() -> std::vector<std::size_t> { return {}; })
          .push_back(idx);
    }
  }
  update_worst(*this);
}

void many_search_state::update([[maybe_unused]] unsigned const k,
                               nigiri::location_idx_t::value_t const l,
                               // duration_t const costs) {
                               delta_t const costs) {
  if (costs == std::numeric_limits<delta_t>::max()) {
    return;
  }
  auto const loc = location_idx_t{l};
  auto const indices = lookup_.find(loc);
  utl::verify(indices != lookup_.end(), "Location {} unreachable by offsets",
              loc);
  auto need_update = false;
  for (auto const idx : indices->second) {
    auto const& offsets = dest_offsets_[idx];
    auto const found = utl::find_if(
        offsets, [&](offset const& offs) { return offs.target() == loc; });
    utl::verify(found != offsets.end(),
                "Failed to find location {} for destination {}", loc, idx);
    auto const total_costs = static_cast<delta_t>(
        costs + found->duration().count());  // TODO Add direction
    if (total_costs < best_[idx]) {
      best_[idx] = total_costs;
      if (worst_.offset_ == idx) {
        need_update = true;
      }
    }
  }
  if (need_update) {
    update_worst(*this);
  }
}

constexpr auto const kVias = via_offset_t{0U};

bitvec to_dest(many_search_state const& state, unsigned int const n_locations) {
  auto d = bitvec{};
  d.resize(n_locations);
  for (auto const& l : state.lookup_) {
    d.set(to_idx(l.first), true);
  }
  return d;
}

std::vector<duration_t> to_durations(many_search_state const& state,
                                     timetable const& tt,
                                     unixtime_t const start_time) {
  auto const base_days = to_base_days(tt, start_time);
  return utl::transform_to<std::vector<duration_t>>(
      state.best_, [&](delta_t const d) -> duration_t {
        return delta_to_unix(base_days, d) - start_time;
      });
}

template <direction SearchDir, bool Rt>
std::vector<duration_t> one_to_many(timetable const& tt,
                                    rt_timetable const* rtt,
                                    many_search_state&& ms_state,
                                    query const& q) {
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

  run_raptor(std::move(algo), tt, start_time, q);

  return to_durations(ms_state, tt, start_time);
}

template <direction SearchDir>
std::vector<duration_t> one_to_many(timetable const& tt,
                                    rt_timetable const* rtt,
                                    many_search_state&& ms_state,
                                    query const& q) {
  if (rtt == nullptr) {
    return one_to_many<SearchDir, false>(tt, rtt, std::move(ms_state), q);
  } else {
    return one_to_many<SearchDir, true>(tt, rtt, std::move(ms_state), q);
  }
}

template std::vector<duration_t> one_to_many<direction::kForward>(
    timetable const&, rt_timetable const*, many_search_state&&, query const&);
template std::vector<duration_t> one_to_many<direction::kBackward>(
    timetable const&, rt_timetable const*, many_search_state&&, query const&);

}  // namespace nigiri::routing
