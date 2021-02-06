#pragma once

#include "cura/relational/rels.h"
#include "utilities/database/database.h"

#include <iostream>
#include <list>

namespace cura::test::execution {

using cura::relational::Rel;
using cura::test::database::Database;

template <typename DbExecutor>
void testExecute(const Database<DbExecutor> &db, const std::string &json) {
  db.explain(json, true);
  db.execute(json);
}

template <typename DbExecutor>
void testExecute(const Database<DbExecutor> &db,
                 std::shared_ptr<const Rel> rel) {
  db.explain(rel, true);
  db.execute(rel);
}

} // namespace cura::test::execution
