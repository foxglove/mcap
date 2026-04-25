#define MCAP_IMPLEMENTATION
#include "mcap/reader.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>

int main(int argc, char* argv[])
{
  if (argc < 2 || argc > 6) {
    fprintf(stderr, "Usage: %s <input_file> [mode] [num_messages] [payload_size] [filter]\n", argv[0]);
    return 1;
  }

  const char* filename = argv[1];
  const char* mode = (argc >= 3) ? argv[2] : "unknown";
  const char* num_messages_str = (argc >= 4) ? argv[3] : "0";
  const char* payload_size_str = (argc >= 5) ? argv[4] : "0";
  const char* filter = (argc >= 6) ? argv[5] : "";

  struct timespec t_start, t_end;
  clock_gettime(CLOCK_MONOTONIC, &t_start);

  mcap::McapReader reader;
  auto res = reader.open(filename);
  if (!res.ok()) {
    fprintf(stderr, "Failed to open %s: %s\n", filename, res.message.c_str());
    return 1;
  }

  auto sres = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
  if (!sres.ok()) {
    fprintf(stderr, "Failed to read summary: %s\n", sres.message.c_str());
    reader.close();
    return 1;
  }

  mcap::ReadMessageOptions opts;
  if (std::strcmp(filter, "topic") == 0) {
    opts.topicFilter = [](std::string_view topic) { return topic == "/imu"; };
  } else if (std::strcmp(filter, "timerange") == 0) {
    opts.startTime = 3000000000;
    opts.endTime = 5000000000;
  } else if (std::strcmp(filter, "topic_timerange") == 0) {
    opts.topicFilter = [](std::string_view topic) { return topic == "/lidar"; };
    opts.startTime = 4000000000;
    opts.endTime = 6000000000;
  }

  long msg_count = 0;
  auto onProblem = [](const mcap::Status& status) {
    fprintf(stderr, "Reader problem: %s\n", status.message.c_str());
  };
  auto messageView = reader.readMessages(onProblem, opts);
  for (auto it = messageView.begin(); it != messageView.end(); ++it) {
    msg_count++;
    /* Touch the data to prevent dead-code elimination */
    if (it->message.dataSize == 0) {
      fprintf(stderr, "Empty message\n");
    }
  }

  reader.close();

  clock_gettime(CLOCK_MONOTONIC, &t_end);

  struct stat st;
  if (stat(filename, &st) != 0) {
    fprintf(stderr, "Failed to stat file\n");
    return 1;
  }
  long file_size = static_cast<long>(st.st_size);

  long long elapsed_ns = (long long)(t_end.tv_sec - t_start.tv_sec) * 1000000000LL
                        + (long long)(t_end.tv_nsec - t_start.tv_nsec);
  double wall_sec = static_cast<double>(elapsed_ns) / 1e9;

  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);

  /* TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb */
  printf("read\tcpp\t%s\t%s\t%s\t%ld\t%lld\t%.6f\t%ld\n",
         mode, num_messages_str, payload_size_str, file_size, elapsed_ns, wall_sec, ru.ru_maxrss);

  (void)msg_count;

  return 0;
}
