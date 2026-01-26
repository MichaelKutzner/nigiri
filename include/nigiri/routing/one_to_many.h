#pragma once

#include <functional>
#include <optional>
#include <vector>

#include "nigiri/routing/query.h"
#include "nigiri/routing/raptor/raptor_state.h"
#include "nigiri/rt/rt_timetable.h"
#include "nigiri/types.h"

namespace nigiri::routing {

template <direction SearchDir>
std::vector<std::optional<duration_t>> one_to_many(
    timetable const& tt,
    rt_timetable const* rtt,
    std::vector<std::vector<offset>> const& dest_offsets,
    query const& q,
    std::optional<std::function<void(raptor_state const&)>> = std::nullopt);

}  // namespace nigiri::routing
