#include "nigiri/routing/one_to_all.h"

#include <chrono>
#include <ranges>
#include <utility>
#include <variant>
#include <vector>

#include "utl/verify.h"

#include "date/date.h"

#include "nigiri/for_each_meta.h"
#include "nigiri/routing/raptor/raptor.h"
#include "nigiri/routing/raptor/run_raptor.h"
#include "nigiri/routing/start_times.h"
#include "nigiri/timetable.h"
#include "nigiri/types.h"

namespace nigiri::routing {

constexpr auto const kVias = via_offset_t{0U};

day_idx_t make_base(timetable const& tt, unixtime_t const start_time) {
  return day_idx_t{std::chrono::duration_cast<date::days>(
                       std::chrono::round<std::chrono::days>(start_time) -
                       tt.internal_interval().from_)
                       .count()};
}

date::sys_days to_base_days(timetable const& tt, unixtime_t const start_time) {
  return tt.internal_interval_days().from_ +
         static_cast<int>(to_idx(make_base(tt, start_time))) * date::days{1};
}

template <direction SearchDir, bool Rt>
raptor_state one_to_all(timetable const& tt,
                        rt_timetable const* rtt,
                        query const& q) {
  utl::verify(std::holds_alternative<unixtime_t>(q.start_time_),
              "Start-time must be a time point (unixtime_t)");
  utl::verify(q.via_stops_.empty(),
              "One-to-All search not supported with vias");
  auto const& start_time = std::get<unixtime_t>(q.start_time_);

  auto state = raptor_state{};

  auto is_dest = bitvec::max(tt.n_locations());
  auto is_via = std::array<bitvec, kMaxVias>{};
  auto dist_to_dest = std::vector<std::uint16_t>{};
  auto lb = std::vector<std::uint16_t>(tt.n_locations(), 0U);
  auto const base = make_base(tt, start_time);
  auto const is_wheelchair = q.prf_idx_ == kWheelchairProfile;

  auto r = raptor<SearchDir, Rt, kVias, search_mode::kOneToAll>{
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
      q.transfer_time_settings_};

  run_raptor(std::move(r), tt, start_time, q);

  return state;
}

template <direction SearchDir>
raptor_state one_to_all(timetable const& tt,
                        rt_timetable const* rtt,
                        query const& q) {
  if (rtt == nullptr) {
    return one_to_all<SearchDir, false>(tt, rtt, q);
  } else {
    return one_to_all<SearchDir, true>(tt, rtt, q);
  }
}

fastest_offset get_fastest_one_to_all_offsets(timetable const& tt,
                                              raptor_state const& state,
                                              direction const search_dir,
                                              location_idx_t const l,
                                              unixtime_t const start_time,
                                              std::uint8_t const transfers) {
  auto const invalid_delta = search_dir == direction::kForward
                                 ? kInvalidDelta<direction::kForward>
                                 : kInvalidDelta<direction::kBackward>;
  auto const& round_times = state.get_round_times<kVias>();
  for (auto const k : std::views::iota(std::uint8_t{0U}, transfers + 2U)  //
                          | std::views::reverse) {
    if (round_times[k][to_idx(l)][kVias] != invalid_delta) {
      auto const base = to_base_days(tt, start_time);
      auto end_time = delta_to_unix(base, round_times[k][to_idx(l)][0]);
      return {
          .duration_ = static_cast<delta_t>((end_time - start_time).count()),
          .k_ = k,
      };
    }
  }
  return {};
};

template raptor_state one_to_all<direction::kForward>(timetable const&,
                                                      rt_timetable const*,
                                                      query const&);
template raptor_state one_to_all<direction::kBackward>(timetable const&,
                                                       rt_timetable const*,
                                                       query const&);

}  // namespace nigiri::routing
