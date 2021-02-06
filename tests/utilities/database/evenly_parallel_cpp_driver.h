#pragma once

#include "cura/common/utilities.h"
#include "cura/driver/driver.h"
#include "database.h"

#include <atomic>
#include <cmath>
#include <future>

namespace cura::test::database {

using cura::driver::Driver;

// TODO: Consider generalize the parallelism and how evenly it would be so that
// all serial/evenly-parallel/oddly-parallel could be represented using the same
// code.
template <typename Sink, size_t parallelism = 8,
          size_t rows_per_heap_thread = 10>
struct EvenlyParallelCppDriver {
  static void
  executePipeline(Driver &driver,
                  const std::unordered_map<TableId, Table> &tables) {
    std::vector<std::future<void>> threads;
    while (driver.pipelineHasNextSource()) {
      auto source_id = driver.pipelineNextSource();
      if (is_heap_source(source_id)) {
        /// Heap source.
        if (driver.isPipelineFinal()) {
          /// Fetch slices of the final heap source.
          for (size_t thread = 0; thread < parallelism; thread++) {
            threads.push_back(
                std::async(std::launch::async, [&driver, source_id, thread]() {
                  auto result = driver.pipelineStream(
                      thread, source_id, nullptr, rows_per_heap_thread);
                  while (result) {
                    Sink::sink(*result);
                    result = driver.pipelineStream(thread, source_id, nullptr,
                                                   rows_per_heap_thread);
                  }
                }));
          }
        } else {
          /// Push the heap source to current pipeline.
          threads.push_back(
              std::async(std::launch::async, [&driver, source_id]() {
                driver.pipelinePush(VoidThreadId, source_id, nullptr);
              }));
        }
      } else {
        /// Input source.
        auto table_it = tables.find(source_id);
        CURA_ASSERT(table_it != tables.end(),
                    "Table " + std::to_string(source_id) + " not found");
        const auto &table = table_it->second;
        size_t fragments_per_thread =
            std::ceil(double(table.fragments.size()) / parallelism);
        for (size_t thread = 0; thread < parallelism; thread++) {
          threads.push_back(
              std::async(std::launch::async, [&driver, source_id, &table,
                                              thread, fragments_per_thread]() {
                for (size_t i = 0; i < fragments_per_thread; i++) {
                  size_t fragment = thread * fragments_per_thread + i;
                  if (fragment < table.fragments.size()) {
                    if (driver.isPipelineFinal()) {
                      /// Stream the input fragment and sink the result.
                      auto result = driver.pipelineStream(
                          thread, source_id, table.fragments[fragment], 0);
                      if (result) {
                        Sink::sink(*result);
                      }
                    } else {
                      /// Push the input fragment into pipeline.
                      driver.pipelinePush(thread, source_id,
                                          table.fragments[fragment]);
                    }
                  }
                }
              }));
        }
      }
    }
    for (auto &thread : threads) {
      thread.get();
    }
  }

  static void explain(std::shared_ptr<const Rel> rel, bool extended) {
    Driver driver;
    auto explain = driver.explain(rel, extended);
    for (const auto &line : explain) {
      std::cout << line << std::endl;
    }
  }

  static void execute(const std::unordered_map<TableId, Table> &tables,
                      std::shared_ptr<const Rel> rel) {
    Driver driver;
    driver.compile(rel);
    driver.setThreadsPerPipeline(parallelism);
    while (driver.hasNextPipeline()) {
      driver.preparePipeline();
      executePipeline(driver, tables);
      driver.finishPipeline();
    }
  }
};

} // namespace cura::test::database
