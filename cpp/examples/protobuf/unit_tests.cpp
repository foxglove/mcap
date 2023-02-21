
#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include "BuildFileDescriptorSet.h"
#include "foxglove/SceneUpdate.pb.h"

using namespace Catch::Matchers;

TEST_CASE("BuildFileDescriptorSet()") {
  SECTION("Does not duplicate dependencies") {
    auto fdSet = foxglove::BuildFileDescriptorSet(foxglove::SceneUpdate::descriptor());

    std::unordered_set<std::string> seenTypeNames;
    for (const auto& file : fdSet.file()) {
      for (const auto& type : file.message_type()) {
        std::string qualifiedName = file.package() + "." + type.name();
        if (seenTypeNames.find(qualifiedName) != seenTypeNames.end()) {
          FAIL("Duplicate message type in FileDescriptorSet: " << qualifiedName);
        }
        seenTypeNames.insert(qualifiedName);
      }
    }

    std::vector<std::string> expectedNames = {
      "foxglove.SceneUpdate",
      "foxglove.SceneEntity",
      "foxglove.SceneEntityDeletion",
      "foxglove.ArrowPrimitive",
      "foxglove.CubePrimitive",
      "foxglove.CylinderPrimitive",
      "foxglove.KeyValuePair",
      "foxglove.LinePrimitive",
      "foxglove.ModelPrimitive",
      "foxglove.SpherePrimitive",
      "foxglove.TextPrimitive",
      "foxglove.TriangleListPrimitive",
      "google.protobuf.Duration",
      "google.protobuf.Timestamp",
      "foxglove.Color",
      "foxglove.Pose",
      "foxglove.Vector3",
      "foxglove.Point3",
      "foxglove.Quaternion",
    };
    REQUIRE_THAT(std::vector(seenTypeNames.begin(), seenTypeNames.end()),
                 UnorderedEquals(expectedNames));
  }
}
