#include "cura/execution/pipeline.h"
#include "cura/common/errors.h"
#include "cura/common/utilities.h"
#include "cura/data/fragment.h"
#include "cura/kernel/sources.h"

#include <sstream>
#include <string>

namespace cura::execution {

namespace detail {

using cura::kernel::Kernel;
using cura::kernel::StreamKernel;
using cura::type::Schema;

struct Node {
  std::vector<std::pair<std::shared_ptr<Node>, size_t>> children;

  std::shared_ptr<const Kernel> kernel;
  size_t total_length = 0;

  explicit Node(std::shared_ptr<const Kernel> kernel_) : kernel(kernel_) {}
};

std::vector<std::shared_ptr<Node>> findParents(
    const std::vector<std::shared_ptr<Node>> &children,
    std::unordered_map<std::shared_ptr<const Kernel>, std::shared_ptr<Node>>
        &visited_map,
    std::vector<std::pair<std::shared_ptr<Node>, size_t>> &roots) {
  std::vector<std::shared_ptr<Node>> parents;
  std::unordered_map<std::shared_ptr<const Kernel>, std::shared_ptr<Node>>
      parent_map;

  for (const auto &child : children) {
    if (auto child_it = visited_map.find(child->kernel);
        child_it != visited_map.end()) {
      auto &visited = child_it->second;
      visited->children.insert(visited->children.end(), child->children.begin(),
                               child->children.end());
      continue;
    }

    auto stream = std::dynamic_pointer_cast<const StreamKernel>(child->kernel);
    if (!stream) {
      roots.emplace_back(child, 0);
    } else if (auto parent_it = parent_map.find(stream->downstream);
               parent_it == parent_map.end()) {
      auto down_stream = stream->downstream;
      auto parent = std::make_shared<Node>(down_stream);
      parent->children.emplace_back(child, 0);

      parents.emplace_back(parent);
      parent_map.emplace(down_stream, parent);
    } else {
      auto parent = parent_it->second;
      parent->children.emplace_back(child, 0);
    }

    visited_map.emplace(child->kernel, child);
  }

  return parents;
}

std::shared_ptr<Node> buildTree(const Pipeline &pipeline) {
  std::vector<std::shared_ptr<Node>> children;
  std::unordered_map<std::shared_ptr<const Kernel>, std::shared_ptr<Node>>
      visited;
  std::vector<std::pair<std::shared_ptr<Node>, size_t>> roots;

  for (auto source_id : pipeline.source_ids) {
    auto source_it = pipeline.sources.find(source_id);
    CURA_ASSERT(source_it != pipeline.sources.end(),
                "Source " + std::to_string(source_id) + " not found");
    auto source_kernel = source_it->second;
    children.emplace_back(std::make_shared<Node>(source_kernel));
  }

  while (!children.empty()) {
    auto parents = findParents(children, visited, roots);
    std::swap(children, parents);
  }

  auto root = std::make_shared<Node>(pipeline.terminal);
  root->children = std::move(roots);
  return root;
}

void alignTreeHorizontal(std::shared_ptr<Node> root) {
  size_t max_length = 0;
  for (auto &child : root->children) {
    alignTreeHorizontal(child.first);
    max_length = std::max(max_length, child.first->total_length);
  }

  if (!root->kernel) {
    root->total_length = max_length;
    return;
  }

  max_length += 3;
  for (auto &child : root->children) {
    child.second += (max_length - child.first->total_length);
  }

  root->total_length = max_length + root->kernel->toString().length();
}

void stringifyTreeHorizontal(
    std::shared_ptr<Node> root, std::vector<std::string> &lines,
    std::unordered_map<std::shared_ptr<Node>, size_t> &line_map) {
  for (auto &child : root->children) {
    stringifyTreeHorizontal(child.first, lines, line_map);
  }

  if (!root->kernel) {
    return;
  }

  if (root->children.empty()) {
    line_map.emplace(root, lines.size());
    lines.emplace_back(root->kernel->toString());
  }

  size_t first_line = 1, last_line = 0;
  for (size_t i = 0; i < root->children.size(); i++) {
    auto line_it = line_map.find(root->children[i].first);
    CURA_ASSERT(line_it != line_map.end(),
                "Couldn't find line for kernel " +
                    root->children[i].first->kernel->toString());
    auto line_id = line_it->second;
    if (i == 0) {
      first_line = line_id;
      last_line = line_id;
    } else {
      CURA_ASSERT(last_line < line_it->second,
                  "Invalid line for kernel " +
                      root->children[i].first->kernel->toString());
      last_line = line_id;
    }
    lines[line_id] += ' ';
    for (size_t j = 1; j < root->children[i].second - 1; j++) {
      lines[line_id] += '-';
    }
    lines[line_id] += ' ';
  }

  size_t length = 0;
  for (size_t i = first_line; i <= last_line; i++) {
    if (i == first_line) {
      length = lines[i].length();
      lines[i] += root->kernel->toString();
    } else {
      for (size_t j = lines[i].length(); j < length; j++) {
        lines[i] += ' ';
      }
      lines[i] += '|';
    }
  }

  line_map.emplace(root, first_line);
}

} // namespace detail

Pipeline::Pipeline(PipelineId id_, bool is_final_,
                   const std::vector<std::shared_ptr<const Source>> &sources_,
                   std::shared_ptr<Terminal> terminal_)
    : id(id_), is_final(is_final_), terminal(terminal_), current_source(0) {
  if (is_final) {
    CURA_ASSERT(!terminal, "Final pipeline must not have terminal");
  } else {
    CURA_ASSERT(terminal, "No terminal in a non-final pipeline");
  }

  for (const auto &source : sources_) {
    auto source_id = source->sourceId();
    source_ids.emplace_back(source_id);
    sources.emplace(source_id, source);
  }
}

bool Pipeline::hasNextSource() const { return current_source < sources.size(); }

SourceId Pipeline::nextSource() {
  CURA_ASSERT(hasNextSource(), "No source left in pipeline");
  return source_ids[current_source++];
}

void Pipeline::push(const Context &ctx, ThreadId thread_id, SourceId source_id,
                    std::shared_ptr<const Fragment> fragment) {
  CURA_ASSERT(!is_final, "push is not allowed for a final pipeline");
  if (isHeapSourceId(source_id)) {
    CURA_ASSERT(!fragment,
                "push an heap source with not-null fragment is not allowed");
  }
  getSource(source_id)->push(ctx, thread_id, VoidKernelId, fragment);
}

std::shared_ptr<const Fragment>
Pipeline::stream(const Context &ctx, ThreadId thread_id, SourceId source_id,
                 std::shared_ptr<const Fragment> fragment, size_t rows) const {
  CURA_ASSERT(is_final, "stream is not allowed for a non-final pipeline");
  if (isHeapSourceId(source_id)) {
    CURA_ASSERT(!fragment,
                "stream an heap source with not-null fragment is not allowed");
  }
  return getSource(source_id)->stream(ctx, thread_id, VoidKernelId, fragment,
                                      rows);
}

std::string Pipeline::toString() const {
  std::stringstream ss;
  ss << "Pipeline#" << id << (is_final ? "(final)" : "") << ":" << std::endl;

  auto root = detail::buildTree(*this);
  alignTreeHorizontal(root);
  std::vector<std::string> lines;
  std::unordered_map<std::shared_ptr<detail::Node>, size_t> line_map;
  stringifyTreeHorizontal(root, lines, line_map);
  for (size_t i = 0; i < lines.size(); i++) {
    ss << lines[i];
    if (i < lines.size() - 1) {
      ss << std::endl;
    }
  }

  return ss.str();
}

std::shared_ptr<const Source> Pipeline::getSource(SourceId source_id) const {
  auto source_it = sources.find(source_id);
  CURA_ASSERT(source_it != sources.end(),
              "Source " + std::to_string(source_id) + " not found");
  return source_it->second;
}

} // namespace cura::execution
