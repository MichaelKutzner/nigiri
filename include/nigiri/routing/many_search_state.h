#pragma once

#include <cstddef>
#include <vector>

#include "utl/enumerate.h"
#include "utl/get_or_create.h"
#include "utl/helpers/algorithm.h"
#include "utl/verify.h"
#include "utl/zip.h"

#include "nigiri/common/delta_t.h"
#include "nigiri/for_each_meta.h"
#include "nigiri/routing/one_to_all.h"
#include "nigiri/routing/pareto_set.h"
#include "nigiri/routing/query.h"
#include "nigiri/routing/raptor/raptor_state.h"
#include "nigiri/rt/rt_timetable.h"
#include "nigiri/types.h"

namespace nigiri::routing {

struct many_search_state {
  struct worst {
    std::size_t offset_{0U};
    delta_t delta_{std::numeric_limits<delta_t>::max()};  // TODO Fix type
  };

  many_search_state(std::vector<std::vector<offset>> const offsets);
  void update(unsigned k, nigiri::location_idx_t::value_t, delta_t);
  // void update(unsigned k, nigiri::location_idx_t, duration_t);

  std::vector<std::vector<offset>> dest_offsets_;
  std::vector<delta_t> best_;
  nigiri::hash_map<nigiri::location_idx_t, std::vector<std::size_t>> lookup_;
  worst worst_;
};

}  // namespace nigiri::routing
