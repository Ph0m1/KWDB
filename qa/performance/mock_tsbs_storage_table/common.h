#pragma once
#include <chrono>

class Timer final {
 public:
  Timer()
      : start_(timestamp_us::now()),
        last_ts_(start_) {
  }

  uint64_t tick() {
    auto now = timestamp_us::now();
    auto elapsed = now - last_ts_;
    last_ts_ = now;
    return elapsed;
  }

  template<typename UNIT>
  struct timestamp {
    static uint64_t now() {
      return std::chrono::duration_cast<UNIT>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    }
  };

  using timestamp_ns [[maybe_unused]] = timestamp<std::chrono::nanoseconds>;
  using timestamp_us [[maybe_unused]] = timestamp<std::chrono::microseconds>;
  using timestamp_ms [[maybe_unused]] = timestamp<std::chrono::milliseconds>;
  using timestamp_s [[maybe_unused]] = timestamp<std::chrono::seconds>;

 private:
  uint64_t start_;
  uint64_t last_ts_;
};

class TestCommon {

 protected:
  void create_db() {
    setenv("KW_HOME", kw_home_.c_str(), 1);
    setenv("KW_IOT_INTERVAL", std::to_string(iot_interval_).c_str(), 1);
    setenv("KW_IOT_MODE", "TRUE", 0);
    BigObjectConfig* config = BigObjectConfig::getBigObjectConfig();
    config->readConfig();
    ErrorInfo err_info;
    std::filesystem::remove_all(kw_home_);
    initBigObjectApplication(err_info);
    string db_path = normalizePath(db_name_);
    string ws = worksapceToDatabase(db_path);
    if (ws.empty()) {
      cerr << "BOEINVALIDNAME" << endl;
      return;
    }
    string dir_path = makeDirectoryPath(BigObjectConfig::home() + ws);
    MakeDirectory(dir_path);
  }

  static void print_statistics(const char* act, size_t call, size_t elapsed) {
    // print statistics
    if (call == 0) {
      return;
    }
    auto row_per_sec = static_cast<double>(call) / (static_cast<double>(elapsed) / 1000000);
    cout << act
         << ": sum(call) = " << call
         << ", sum(time) = " << elapsed << " us"
         << ", T(Q)PS = " << row_per_sec << endl;
  }

 protected:
  string kw_home_ = "perf_test/";
  string db_name_;
  uint64_t iot_interval_;
};