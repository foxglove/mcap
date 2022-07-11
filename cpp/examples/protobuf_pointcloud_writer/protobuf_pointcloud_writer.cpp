#include <chrono>
#include <cmath>
#include <random>

#define MCAP_IMPLEMENTATION
#include <mcap/writer.hpp>

// Utility class to generate random points on a sphere on demand.
class PointGenerator {
private:
  std::mt19937 _generator;
  std::uniform_real_distribution<double> _distribution;

public:
  PointGenerator(uint32_t seed = 0)
      : _generator(seed)
      , _distribution(0.0, 1.0) {}

  Point write(float scale, float* x, float* y, float* z) {
    float theta = 2 * M_PI * _distribution(_generator);
    float phi = acos(1 - 2 * _distribution(_generator));
    *x = (sin(phi) * cos(theta)) * scale;
    *y = (sin(phi) * sin(theta)) * scale;
    *z = cos(phi) * scale;
  }
}
