#include "gtest/gtest.h"

#include <chrono>
#include <string_view>

#include "date/date.h"

#include "nigiri/loader/gtfs/load_timetable.h"
#include "nigiri/loader/init_finish.h"
#include "nigiri/common/interval.h"

#include "../raptor_search.h"

using namespace date;
using namespace nigiri;
using namespace std::chrono_literals;
using namespace std::string_view_literals;

namespace {

// Duplicated from 'optimize_footpaths.cc'
[[maybe_unused]] std::string print_journey(timetable const& tt,
                                           routing::journey const& journey) {
  std::stringstream ss;
  ss << "\n";
  journey.print(ss, tt);
  return ss.str();
}

[[maybe_unused]] timetable load_timetable(std::string_view s) {
  auto tt = timetable{};

  tt.date_range_ = {date::sys_days{2019_y / May / 1},
                    date::sys_days{2019_y / May / 2}};
  loader::register_special_stations(tt);
  loader::gtfs::load_timetable({}, source_idx_t{0}, loader::mem_dir::read(s),
                               tt);
  loader::finalize(tt);
  return tt;
}

location_idx_t loc_idx(timetable const& tt, std::string_view const id) {
  return tt.find(location_id{id, source_idx_t{0}}).value();
}

/*

A--\                  /--B
    \   /-I-\        /
     N-+-----+-M----L
    /                \
F--/                  \--E

S1 -> RE: Multiple possible transfers for A -> E
S1 -> S2: Multiple possible transfers for A -> F
*/

constexpr auto const test_files_1 = R"(
# agency.txt
agency_id,agency_name,agency_url,agency_timezone
DB,Deutsche Bahn,https://deutschebahn.com,Europe/Berlin

#stops.txt
stop_id,stop_name,stop_desc,stop_lon,stop_lat,location_type,parent_station
A,A,,0.0,0.0,,
B,B,,6.0,0.0,,
E,E,,6.0,4.0,,
F,F,,0.0,4.0,,
M,Main,,3.0,2.0,,
N,Near,,2.0,2.0,,
L,Last,,4.0,2.0,,
I,Interior,,2.5,1.5,,

#routes.txt
route_id,agency_id,route_short_name,route_long_name,route_desc,route_type
RE,DB,RE,,,2
S1,DB,S-Forward,,,0
S2,DB,S-Backward,,,0

#trips.txt
route_id,service_id,trip_id,trip_headsign,block_id
RE,S1,RE1,,
S1,S1,S1a,,
S1,S1,S1b,,
S2,S1,S2a,,
S2,S1,S2b,,


#stop_times.txt
trip_id,arrival_time,departure_time,stop_id,stop_sequence
RE1,10:00:00,10:00:00,F,0
RE1,10:40:00,10:41:00,N,1
RE1,10:50:00,11:00:00,M,2
RE1,11:10:00,11:11:00,L,3
RE1,12:00:00,12:00:00,E,4
S1a,10:00:00,10:00:00,A,0
S1a,10:35:00,10:36:00,N,1
S1a,10:42:00,10:43:00,I,2
S1a,10:50:00,10:53:00,M,3
S1a,11:03:00,11:05:00,L,4
S1a,12:00:00,12:00:00,B,5

#calendar_dates.txt
service_id,date,exception_type
S1,20190501,1

#transfers.txt
from_stop_id,to_stop_id,transfer_type,min_transfer_time
)"sv;

}  // namespace

// Prefer stop with largest time for transfers
TEST(routing, better_transfers_same_direction) {
  auto tt = load_timetable(test_files_1);
  // fmt::println("DEBUG: #loc: {}  #ags: {}  #route: {}  #trip: {}",
  //              tt.n_locations(), tt.n_agencies(), tt.n_routes(),
  //              tt.n_trips());

  auto const A = loc_idx(tt, "A");
  auto const E = loc_idx(tt, "E");

  auto q = routing::query{
      .start_time_ = interval{.from_ = date::sys_days{2019_y / May / 1},
                              .to_ = date::sys_days{2019_y / May / 2}},
      .start_match_mode_ = nigiri::routing::location_match_mode::kIntermodal,
      .dest_match_mode_ = nigiri::routing::location_match_mode::kIntermodal,
      .start_ = {{A, 1_minutes, 0U}},
      .destination_ = {{E, 2_minutes, 0U}}};
  auto const results = test::raptor_search(tt, nullptr, std::move(q));

  constexpr auto const expected_better_transfer = R"(
[2019-05-01 07:59, 2019-05-01 10:02]
TRANSFERS: 1
     FROM: (START, START) [2019-05-01 07:59]
       TO: (END, END) [2019-05-01 10:02]
leg 0: (START, START) [2019-05-01 07:59] -> (A, A) [2019-05-01 08:00]
  MUMO (id=0, duration=1)
leg 1: (A, A) [2019-05-01 08:00] -> (Last, L) [2019-05-01 09:03]
   0: A       A...............................................                               d: 01.05 08:00 [01.05 10:00]  [{name=S-Forward, day=2019-05-01, id=S1a, src=0}]
   1: N       Near............................................ a: 01.05 08:35 [01.05 10:35]  d: 01.05 08:36 [01.05 10:36]  [{name=S-Forward, day=2019-05-01, id=S1a, src=0}]
   2: I       Interior........................................ a: 01.05 08:42 [01.05 10:42]  d: 01.05 08:43 [01.05 10:43]  [{name=S-Forward, day=2019-05-01, id=S1a, src=0}]
   3: M       Main............................................ a: 01.05 08:50 [01.05 10:50]  d: 01.05 08:53 [01.05 10:53]  [{name=S-Forward, day=2019-05-01, id=S1a, src=0}]
leg 2: (Main, M) [2019-05-01 08:53] -> (Main, M) [2019-05-01 08:55]
  FOOTPATH (duration=2)
leg 3: (Main, M) [2019-05-01 09:00] -> (E, E) [2019-05-01 10:00]
   2: M       Main............................................ a: 01.05 09:00 [01.05 11:00]
   3: L       Last............................................                               d: 01.05 09:11 [01.05 11:11]  [{name=RE, day=2019-05-01, id=RE1, src=0}]
   4: E       E............................................... a: 01.05 10:00 [01.05 12:00]
leg 4: (E, E) [2019-05-01 10:00] -> (END, END) [2019-05-01 10:02]
  MUMO (id=0, duration=2)


)"sv;
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(expected_better_transfer,
            test::print_results(tt, nullptr, results));
  // EXPECT_EQ(expected_better_transfer, print_journey(tt, results.front()));
}

// Prefer stop with largest time for transfers
TEST(routing, better_transfers_opposite_direction) {
  // TODO: MK - Implement
}
