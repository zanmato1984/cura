#pragma once

#include "table.h"

#include <iostream>

namespace cura::test::database {

template <typename DbExecutor> class Database {
public:
  explicit Database(std::vector<Table> tables_) {
    for (auto &table : tables_) {
      Table t = std::move(table);
      tables.emplace(t.id, std::move(t));
    }
  }

  template <typename T> void explain(T &&plan, bool extended) const {
    DbExecutor::explain(std::forward<T>(plan), extended);
  }

  template <typename T> void execute(T &&plan) const {
    DbExecutor::execute(tables, std::forward<T>(plan));
  }

private:
  std::unordered_map<TableId, Table> tables;
};

} // namespace cura::test::database
