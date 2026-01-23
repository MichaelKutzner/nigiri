#include "nigiri/routing/raptor/raptor_state.h"
#include "nigiri/loader/dir.h"

#include <algorithm>
#include <vector>

#include "fmt/core.h"

#include "utl/enumerate.h"
#include "utl/get_or_create.h"
#include "utl/helpers/algorithm.h"
#include "utl/zip.h"

#include "nigiri/common/delta_t.h"
#include "nigiri/routing/limits.h"
#include "nigiri/timetable.h"
#include "nigiri/types.h"

namespace nigiri::routing {

bool is_better(delta_t const a, delta_t const b, direction const dir) {
  return dir == direction::kForward ? a < b : b < a;
}

delta_t dir(delta_t const a, direction const dir) {
  return dir == direction::kForward ? a : (-1) * a;
}

delta_t max_delta(direction const dir) {
  return dir == direction::kForward ? kInvalidDelta<direction::kForward>
                                    : kInvalidDelta<direction::kBackward>;
}

delta_t min_delta(direction const dir) {
  return dir == direction::kForward ? kInvalidDelta<direction::kBackward>
                                    : kInvalidDelta<direction::kForward>;
}

void update_worst(raptor_state::many_search& state) {
  auto idx = std::size_t{0U};
  state.worst_.delta_ = min_delta(state.dir_);
  for (auto const [offs, best] : utl::zip(state.dest_offsets_, state.best_)) {
    if (!offs.empty()) {  // Ignore all entries without reachable location
      if (is_better(state.worst_.delta_, best, state.dir_)) {
        state.worst_.delta_ = best;
        state.worst_.offset_ = idx;
      }
    }
    ++idx;
  }
}

raptor_state::many_search::many_search(
    std::vector<std::vector<offset>> const& dest_offsets, direction const dir)
    : dest_offsets_{dest_offsets},
      best_{std::vector(dest_offsets_.size(), max_delta(dir))},
      worst_{},
      dir_{dir} {
  for (auto const [idx, dest] : utl::enumerate(dest_offsets_)) {
    for (auto const& offset : dest) {
      utl::get_or_create(lookup_, offset.target(),
                         []() -> std::vector<std::size_t> { return {}; })
          .push_back(idx);
    }
  }
  update_worst(*this);
}

delta_t raptor_state::many_search::update([[maybe_unused]] unsigned const k,
                                          location_idx_t::value_t const l,
                                          delta_t const costs) {
  if (costs == max_delta(dir_)) {
    return worst_.delta_;
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
    auto const total_costs =
        static_cast<delta_t>(costs + dir(found->duration().count(), dir_));
    if (is_better(total_costs, best_[idx], dir_)) {
      best_[idx] = total_costs;
      if (worst_.offset_ == idx) {
        need_update = true;
      }
    }
  }
  if (need_update) {
    update_worst(*this);
  }
  return worst_.delta_;
}

raptor_state& raptor_state::resize(unsigned const n_locations,
                                   unsigned const n_routes,
                                   unsigned const n_rt_transports) {
  n_locations_ = n_locations;
  tmp_storage_.resize(n_locations * (kMaxVias + 1));
  best_storage_.resize(n_locations * (kMaxVias + 1));
  round_times_storage_.resize(n_locations * (kMaxVias + 1) *
                              (kMaxTransfers + 2));
  station_mark_.resize(n_locations);
  prev_station_mark_.resize(n_locations);
  route_mark_.resize(n_routes);
  rt_transport_mark_.resize(n_rt_transports);
  return *this;
}

template <via_offset_t Vias>
void raptor_state::print(timetable const& tt,
                         date::sys_days const base,
                         delta_t const invalid) {
  auto invalid_array = std::array<delta_t, Vias + 1>{};
  invalid_array.fill(invalid);

  auto const& tmp = get_tmp<Vias>();
  auto const& best = get_best<Vias>();
  auto const& round_times = get_round_times<Vias>();

  auto const has_empty_rounds = [&](std::uint32_t const l) {
    for (auto k = 0U; k != kMaxTransfers + 2U; ++k) {
      if (round_times[k][l] != invalid_array) {
        return false;
      }
    }
    return true;
  };

  auto const print_delta = [&](delta_t const d) {
    if (d == invalid) {
      fmt::print("________________");
    } else {
      fmt::print("{:16}", delta_to_unix(base, d));
    }
  };

  auto const print_deltas = [&](std::array<delta_t, Vias + 1> const& deltas) {
    fmt::print("[ ");
    for (auto const d : deltas) {
      print_delta(d);
      fmt::print(" ");
    }
    fmt::print("]");
  };

  for (auto l = 0U; l != tt.n_locations(); ++l) {
    if (best[l] == invalid_array && has_empty_rounds(l)) {
      continue;
    }

    fmt::print("{:80}  ", fmt::streamed(loc{tt, location_idx_t{l}}));

    fmt::print("tmp=");
    print_deltas(tmp[l]);
    fmt::print(", ");

    auto const& b = best[l];
    fmt::print("best=");
    print_deltas(b);
    fmt::print(", round_times: ");
    for (auto i = 0U; i != kMaxTransfers + 2U; ++i) {
      auto const& t = round_times[i][l];
      fmt::print("{}:", i);
      print_deltas(t);
      fmt::print(" ");
    }
    fmt::print("\n");
  }
}

static_assert(kMaxVias == 2,
              "raptor_state.cc needs to be adjusted for kMaxVias");

template void raptor_state::print<0>(timetable const& tt,
                                     date::sys_days const base,
                                     delta_t const invalid);

template void raptor_state::print<1>(timetable const& tt,
                                     date::sys_days const base,
                                     delta_t const invalid);

template void raptor_state::print<2>(timetable const& tt,
                                     date::sys_days const base,
                                     delta_t const invalid);

}  // namespace nigiri::routing
