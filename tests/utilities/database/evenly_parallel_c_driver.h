#pragma once

#include "cura/c/api.h"
#include "database.h"

#include <arrow/c/bridge.h>
#include <atomic>
#include <cmath>
#include <future>

namespace cura::test::database {

#define CURA_ASSERT_C(cond)                                                    \
  {                                                                            \
    auto error = (cond);                                                       \
    CURA_ASSERT(!error,                                                        \
                "CURA C API call failed with error " + std::to_string(error)); \
  }

// TODO: Consider generalize the parallelism and how evenly it would be so that
// all serial/evenly-parallel/oddly-parallel could be represented using the same
// code.
template <typename Sink, size_t parallelism = 8,
          size_t rows_per_heap_thread = 10>
struct EvenlyParallelCDriver {
  static void
  executePipeline(DriverHandle driver,
                  const std::unordered_map<TableId, Table> &tables) {
    std::vector<std::future<void>> threads;
    while (pipeline_has_next_source(driver)) {
      auto source_id = pipeline_next_source(driver);
      if (is_heap_source(source_id)) {
        /// Heap source.
        if (is_pipeline_final(driver)) {
          /// Fetch slices of the final heap source.
          for (size_t thread = 0; thread < parallelism; thread++) {
            threads.push_back(
                std::async(std::launch::async, [driver, source_id, thread]() {
                  ArrowArray output{};
                  ArrowSchema schema{};

                  auto res = pipeline_stream(driver, thread, source_id, nullptr,
                                             nullptr, &output, &schema,
                                             rows_per_heap_thread);
                  CURA_ASSERT(res >= 0, "Pipeline stream failed with error " +
                                            std::to_string(res));

                  while (res > 0) {
                    auto rb = CURA_GET_ARROW_RESULT(
                        arrow::ImportRecordBatch(&output, &schema));
                    auto result = std::make_shared<Fragment>(rb);
                    Sink::sink(*result);
                    if (output.release) {
                      output.release(&output);
                    }
                    if (schema.release) {
                      schema.release(&schema);
                    }

                    res = pipeline_stream(driver, thread, source_id, nullptr,
                                          nullptr, &output, &schema,
                                          rows_per_heap_thread);
                    CURA_ASSERT(res >= 0, "Pipeline stream failed with error " +
                                              std::to_string(res));
                  }
                }));
          }
        } else {
          /// Push the heap source to current pipeline.
          threads.push_back(std::async(std::launch::async, [&driver,
                                                            source_id]() {
            pipeline_push(driver, VoidThreadId, source_id, nullptr, nullptr);
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
              std::async(std::launch::async, [driver, source_id, &table, thread,
                                              fragments_per_thread]() {
                for (size_t i = 0; i < fragments_per_thread; i++) {
                  size_t fragment_id = thread * fragments_per_thread + i;
                  if (fragment_id < table.fragments.size()) {
                    const auto &fragment = table.fragments[fragment_id];
                    auto input_rb = fragment->arrow();
                    ArrowArray input{};
                    ArrowSchema input_schema{};
                    CURA_ASSERT_ARROW_OK(arrow::ExportRecordBatch(
                                             *input_rb, &input, &input_schema),
                                         "Export record batch failed");

                    if (is_pipeline_final(driver)) {
                      /// Stream the input fragment and sink the result.
                      ArrowArray output{};
                      ArrowSchema output_schema{};
                      auto res = pipeline_stream(driver, thread, source_id,
                                                 &input, &input_schema, &output,
                                                 &output_schema, 0);
                      CURA_ASSERT(res >= 0,
                                  "Pipeline stream failed with error " +
                                      std::to_string(res));
                      if (res > 0) {
                        auto output_rb = CURA_GET_ARROW_RESULT(
                            arrow::ImportRecordBatch(&output, &output_schema));
                        auto result = std::make_shared<Fragment>(output_rb);
                        Sink::sink(*result);
                        if (output.release) {
                          output.release(&output);
                        }
                        if (output_schema.release) {
                          output_schema.release(&output_schema);
                        }
                      }
                    } else {
                      /// Push the input fragment into pipeline.
                      CURA_ASSERT_C(pipeline_push(driver, thread, source_id,
                                                  &input, &input_schema));
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

  static void explain(const std::string &json, bool extended) {
    auto driver = create_driver();
    CURA_ASSERT(driver, "Create driver failed");

    char **explained;
    int n = ::explain(driver, json.data(), extended, &explained);
    CURA_ASSERT(n >= 0, "Explain failed with " + std::to_string(n));
    for (int i = 0; i < n; i++) {
      std::cout << explained[i] << std::endl;
      free(explained[i]);
    }
    free(explained);
  }

  static void execute(const std::unordered_map<TableId, Table> &tables,
                      const std::string &json) {
    auto driver = create_driver();
    CURA_ASSERT(driver, "Create driver failed");

    CURA_ASSERT_C(compile(driver, json.data()));

    CURA_ASSERT_C(set_threads_per_pipeline(driver, parallelism));

    while (has_next_pipeline(driver)) {
      CURA_ASSERT_C(prepare_pipeline(driver));
      executePipeline(driver, tables);
      CURA_ASSERT_C(finish_pipeline(driver));
    }

    release_driver(driver);
  }
};

#undef CURA_ASSERT_C

} // namespace cura::test::database
