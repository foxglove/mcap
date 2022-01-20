#include <mcap/mcap.hpp>

#include <iostream>

int main() {
  mcap::Message msg;
  msg.channelId = 1;
  msg.sequence = 2;
  msg.publishTime = 3;
  msg.recordTime = 4;
  msg.data.push_back(5);

  std::cout << "msg.channelId = " << msg.channelId << "\n";
  return 0;
}
