#include "nigiri/routing/one_to_many.h"

#include "utl/verify.h"

#include "nigiri/routing/one_to_all.h"
#include "nigiri/routing/raptor/run_raptor.h"
#include "nigiri/types.h"

namespace nigiri::routing {

constexpr auto const kVias = via_offset_t{0U};

bitvec to_dest(raptor_state::many_search const& many,
               unsigned int const n_locations) {
  auto d = bitvec{};
  d.resize(n_locations);
  for (auto const& l : many.lookup_) {
    d.set(to_idx(l.first), true);
  }
  return d;
}

template <direction SearchDir, bool Rt>
std::vector<duration_t> one_to_many(
    timetable const& tt,
    rt_timetable const* rtt,
    std::vector<std::vector<offset>> const& dest_offsets,
    query const& q,
    std::optional<std::function<void(raptor_state const&)>> cb) {
  utl::verify(std::holds_alternative<unixtime_t>(q.start_time_),
              "Start-time must be a time point (unixtime_t)");
  utl::verify(q.via_stops_.empty(),
              "One-to-All search not supported with vias");
  auto const& start_time = std::get<unixtime_t>(q.start_time_);

  auto many = raptor_state::many_search{dest_offsets, SearchDir};
  auto state = raptor_state{many};

  auto is_dest = to_dest(many, tt.n_locations());
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
      q.transfer_time_settings_};

  run_raptor(std::move(algo), tt, start_time, q);

  if (cb) {
    cb->operator()(state);
  }

  return many.durations(tt, start_time);
}

template <direction SearchDir>
std::vector<duration_t> one_to_many(
    timetable const& tt,
    rt_timetable const* rtt,
    std::vector<std::vector<offset>> const& dest_offsets,
    query const& q,
    std::optional<std::function<void(raptor_state const&)>> cb) {
  if (rtt == nullptr) {
    return one_to_many<SearchDir, false>(tt, rtt, dest_offsets, q, cb);
  } else {
    return one_to_many<SearchDir, true>(tt, rtt, dest_offsets, q, cb);
  }
}

template std::vector<duration_t> one_to_many<direction::kForward>(
    timetable const&,
    rt_timetable const*,
    std::vector<std::vector<offset>> const& dest_offsets,
    query const& q,
    std::optional<std::function<void(raptor_state const&)>>);
template std::vector<duration_t> one_to_many<direction::kBackward>(
    timetable const&,
    rt_timetable const*,
    std::vector<std::vector<offset>> const& dest_offsets,
    query const& q,
    std::optional<std::function<void(raptor_state const&)>>);

}  // namespace nigiri::routing
