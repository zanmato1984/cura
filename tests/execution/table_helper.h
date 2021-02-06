#pragma once

#include "utilities/data_helper.h"

using cura::data::ColumnVector;
using cura::test::data::makeArrowColumnVector;
using cura::test::data::makeArrowColumnVectorN;
using cura::test::data::makeFragment;
using cura::test::database::Table;
using cura::test::database::TableId;
using cura::type::DataType;

template <typename T>
Table makeTable(const DataType &data_type, TableId id, size_t num_fragments,
                const std::vector<T> &data) {
  Table t{id};
  auto tmp = data;
  std::shared_ptr<const ColumnVector> cv =
      makeArrowColumnVector(data_type, move(tmp));
  for (size_t i = 0; i < num_fragments; i++) {
    t.fragments.emplace_back(makeFragment(cv));
  }
  return t;
}

template <typename T>
Table makeTableN(const DataType &data_type, TableId id, size_t num_fragments,
                 size_t n, T start = 0) {
  Table t{id};
  std::shared_ptr<const ColumnVector> cv =
      makeArrowColumnVectorN(data_type, n, start);
  for (size_t i = 0; i < num_fragments; i++) {
    t.fragments.emplace_back(makeFragment(cv));
  }
  return t;
}
