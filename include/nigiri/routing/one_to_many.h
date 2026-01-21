#pragma once

#include <vector>

#include "utl/to_vec.h"
#include "utl/verify.h"

#include "nigiri/common/delta_t.h"
#include "nigiri/for_each_meta.h"
#include "nigiri/routing/many_search_state.h"
#include "nigiri/routing/one_to_all.h"
#include "nigiri/routing/pareto_set.h"
#include "nigiri/routing/query.h"
#include "nigiri/routing/raptor/raptor.h"
#include "nigiri/routing/raptor/raptor_state.h"
#include "nigiri/rt/rt_timetable.h"
#include "nigiri/types.h"

namespace nigiri::routing {

template <direction SearchDir>
std::vector<duration_t> one_to_many(timetable const& tt,
                                    rt_timetable const* rtt,
                                    many_search_state&& ms_state,
                                    query const& q);

}  // namespace nigiri::routing
