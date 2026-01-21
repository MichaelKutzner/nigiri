#include "nigiri/routing/one_to_many.h"
#include "nigiri/common/delta_t.h"
#include "nigiri/routing/many_search_state.h"

#include <cstddef>
#include <limits>
#include <vector>

#include "utl/enumerate.h"
#include "utl/get_or_create.h"
#include "utl/helpers/algorithm.h"
#include "utl/verify.h"
#include "utl/zip.h"

#include "nigiri/routing/query.h"
#include "nigiri/types.h"

namespace nigiri::routing {

void update_worst(many_search_state& state) {
  auto idx = std::size_t{0U};
  // state.worst_.duration_ = duration_t{0U};
  state.worst_.duration_ = std::numeric_limits<delta_t>::min();
  for (auto const [offs, best] : utl::zip(state.dest_offsets_, state.best_)) {
    if (!offs.empty()) {  // Ignore all entries without reachable location
      if (best > state.worst_.duration_) {
        state.worst_.duration_ = best;
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
      // best_{std::vector(dest_offsets_.size(), duration_t::max())},
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
        static_cast<delta_t>(costs + found->duration().count());  // TODO Add direction
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

}  // namespace nigiri::routing
