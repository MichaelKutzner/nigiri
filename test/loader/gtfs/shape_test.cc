#include <filesystem>
#include <iostream>
#include <numeric>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "gtest/gtest.h"

#include "geo/latlng.h"
#include "utl/raii.h"

#include "nigiri/loader/gtfs/shape.h"

// #include "./test_data.h"

using namespace nigiri::loader::gtfs;

void cleanup_paths(ShapeMap::Paths const& paths) {
  for (auto path : std::vector<std::filesystem::path>{
           paths.id_file, paths.shape_data_file, paths.shape_metadata_file}) {
    if (std::filesystem::exists(path)) {
      std::filesystem::remove(path);
    }
  }
}

ShapeMap::Paths get_paths(std::string base_path) {
  return {
      base_path + "-id.dat",
      base_path + "-shape-data.dat",
      base_path + "-shape-metadata.dat",
  };
}

TEST(gtfs, shapeConstruct_createData_canAccessData) {
  std::string shapes_data{
      R"("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"
243,51.543652,7.217830,0
243,51.478609,7.223275,1
3105,50.553822,6.356876,0
3105,50.560999,6.355028,1
3105,50.560999,6.355028,2
3105,50.565724,6.364605,3
3105,50.578249,6.383394,7
3105,50.578249,6.383394,8
3105,50.581956,6.379866,11
)"};
  auto paths{get_paths("shape-test-create")};
  auto guard = utl::make_raii(paths, cleanup_paths);

  ShapeMap shapes(shapes_data, paths);

  std::vector<std::vector<geo::latlng>> shape_points{
      {
          {51.543652, 7.217830},
          {51.478609, 7.223275},
      },
      {
          {50.553822, 6.356876},
          {50.560999, 6.355028},
          {50.560999, 6.355028},
          {50.565724, 6.364605},
          {50.578249, 6.383394},
          {50.578249, 6.383394},
          {50.581956, 6.379866},
      },
  };
  EXPECT_EQ(2, shapes.size());
  EXPECT_TRUE(shapes.contains("243"));
  EXPECT_TRUE(shapes.contains("3105"));
  EXPECT_FALSE(shapes.contains("1234"));
  EXPECT_EQ(shape_points.at(0), shapes.at("243"));
  EXPECT_EQ(shape_points.at(1), shapes.at("3105"));
  size_t loop_count{};
  for (auto const [id, shape] : shapes) {
    if (id == "243") {
      EXPECT_EQ(shape_points.at(0), shape);
    } else {
      EXPECT_EQ("3105", id);
      EXPECT_EQ(shape_points.at(1), shape);
    }
    // Reminder: Internal order can be random
    ++loop_count;
  }
  EXPECT_EQ(2, loop_count);
  auto points_total = std::accumulate(
      shapes.begin(), shapes.end(), 0u,
      [](auto sum, auto shape) { return sum + shape.second.size(); });
  EXPECT_EQ(9, points_total);
}

TEST(gtfs, shapeParse_validIDs_parseData) {
  std::string shapes_data{
      R"("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"
test id,50.553822,6.356876,0
----,50.560999,6.355028,1
)"
      "\x07\x13\x41\x08"
      R"(,50.560999,6.355028,2
ルーティング,50.565724,6.364605,3
,50.565724,6.364605,4
🚀,51.543652,7.217830,0
🚏,51.478609,7.223275,1
)"};
  auto paths{get_paths("shape-test-valid-ids")};
  auto guard = utl::make_raii(paths, cleanup_paths);

  ShapeMap shapes(shapes_data, paths);

  EXPECT_EQ(7, shapes.size());
  EXPECT_TRUE(shapes.contains("test id"));
  EXPECT_TRUE(shapes.contains("----"));
  EXPECT_TRUE(shapes.contains("\x07\x13\x41\x08"));
  EXPECT_TRUE(shapes.contains("ルーティング"));
  EXPECT_TRUE(shapes.contains(""));
  EXPECT_TRUE(shapes.contains("🚀"));
  EXPECT_TRUE(shapes.contains("🚏"));
}

TEST(gtfs, shapeParse_randomColumOrder_parseCorrectly) {
  std::string shapes_data{
      R"("shape_pt_sequence","shape_pt_lon","shape_id","shape_pt_lat"
6,6.089410,123,50.767212
74,6.074227,123,50.775187
230,6.094470,123,50.871905
277,6.070844,123,50.890206
339,6.023209,123,50.896437
367,5.995949,123,50.890583
410,5.978670,123,50.890088
481,5.909033,123,50.879289
663,5.705982,123,50.849446
721,5.716989,123,50.838980
)"};
  auto paths{get_paths("shape-test-random-column-order")};
  auto guard = utl::make_raii(paths, cleanup_paths);

  ShapeMap shapes(shapes_data, paths);

  EXPECT_EQ(1, shapes.size());
  EXPECT_TRUE(shapes.contains("123"));
  EXPECT_EQ(10, shapes.at("123").size());
}

TEST(gtfs, shapeParse_notAscendingSequence_throwException) {
  std::string shapes_data{
      R"("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"
1,50.636512,6.473487,1
1,50.636259,6.473668,0
)"};
  auto paths{get_paths("shape-test-not-ascending-sequence")};
  auto guard = utl::make_raii(paths, cleanup_paths);
  std::stringstream buffer{};
  auto backup = std::clog.rdbuf(buffer.rdbuf());
  auto buffer_guard = utl::make_raii(
      backup, [](const decltype(backup)& buf) { std::clog.rdbuf(buf); });

  ShapeMap shapes(shapes_data, paths);

  std::clog.flush();
  std::string_view log{buffer.str()};
  // std::cout << "Full log??: >" << log << "<" << std::endl;
  EXPECT_TRUE(
      log.contains("Non monotonic sequence for shape_id '1': Sequence number 1 "
                   "followed by 0"));
}

// // Currently not testable
// TEST(gtfs, shapeParse_missingColumn_throwException) {
//     std::string shapes_data{R"("shape_id","shape_pt_lat","shape_pt_sequence"
// 1,50.636259,0
// )"};
//     auto paths{get_paths("shape-test-missing-column")};
//   auto guard = utl::make_raii(paths, cleanup_paths);

//     EXPECT_THROW(ShapeMap shapes(shapes_data, paths), InvalidShapesFormat);
// }

TEST(gtfs, shapeParse_shuffledRows_parseAllData) {
  std::string shapes_data{
      R"("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"
234,51.473214,7.139521,0
241,51.504903,7.102455,0
241,51.473214,7.139521,1
243,51.543652,7.217830,0
244,51.473214,7.139521,0
244,51.504903,7.102455,1
243,51.478609,7.223275,1
235,51.478609,7.223275,0
234,51.459894,7.153535,1
240,51.459894,7.153535,0
240,51.473214,7.139521,1
235,51.543652,7.217830,1
)"};
  auto paths{get_paths("shape-test-shuffled-rows")};
  auto guard = utl::make_raii(paths, cleanup_paths);

  ShapeMap shapes(shapes_data, paths);

  std::unordered_map<std::string, std::vector<geo::latlng>> shape_points{
      {"240",
       {
           {51.459894, 7.153535},
           {51.473214, 7.139521},
       }},
      {"234",
       {
           {51.473214, 7.139521},
           {51.459894, 7.153535},
       }},
      {"244",
       {
           {51.473214, 7.139521},
           {51.504903, 7.102455},
       }},
      {"235",
       {
           {51.478609, 7.223275},
           {51.543652, 7.217830},
       }},
      {"241",
       {
           {51.504903, 7.102455},
           {51.473214, 7.139521},
       }},
      {"243",
       {
           {51.543652, 7.217830},
           {51.478609, 7.223275},
       }},
  };
  EXPECT_EQ(shape_points.size(), shapes.size());
  for (auto [id, coordinates] : shape_points) {
    EXPECT_TRUE(shapes.contains(id));
    EXPECT_EQ(coordinates, shapes.at(id));
  }
}

TEST(gtfs, shapeParse_delayedInsertWithNotAscendingSequence_throwException) {
  std::string shapes_data{
      R"("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"
1,50.636512,6.473487,1
2,51.473214,7.139521,0
1,50.636259,6.473668,0
)"};
  auto paths{get_paths("shape-test-not-ascending-sequence")};
  auto guard = utl::make_raii(paths, cleanup_paths);
  std::stringstream buffer{};
  auto backup = std::clog.rdbuf(buffer.rdbuf());
  auto buffer_guard = utl::make_raii(
      backup, [](const decltype(backup)& buf) { std::clog.rdbuf(buf); });

  ShapeMap shapes(shapes_data, paths);

  std::clog.flush();
  std::string_view log{buffer.str()};
  EXPECT_TRUE(
      log.contains("Non monotonic sequence for shape_id '1': Sequence number 1 "
                   "followed by 0"));
}

TEST(gtfs, shapeParse_idWithNullByte_removeNullByteFromId) {
  using std::literals::operator""s;
  std::string shapes_data{
      R"("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"
null)"
      "\0"s
      R"(byte,51.543652,7.217830,0
other,50.553822,6.356876,0
)"};
  auto paths{get_paths("shape-test-id-with-null-byte")};
  auto guard = utl::make_raii(paths, cleanup_paths);

  ShapeMap shapes(shapes_data, paths);

  EXPECT_EQ(2, shapes.size());
  EXPECT_TRUE(shapes.contains("null\0byte"s));
  EXPECT_FALSE(shapes.contains("nullbyte"));
  EXPECT_TRUE(shapes.contains("other"));
}