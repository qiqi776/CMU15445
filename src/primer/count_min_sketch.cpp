//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("CountMinSketch width and depth must be greater than 0.");
  }

  const auto total_buckets = static_cast<size_t>(width_) * static_cast<size_t>(depth_);
  counters_ = std::make_unique<std::atomic<uint32_t>[]>(total_buckets);

  for (size_t i = 0; i < total_buckets; i++) {
    counters_[i].store(0, std::memory_order_relaxed);
  }
  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_), depth_(other.depth_), counters_(std::move(other.counters_)) {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  other.width_ = 0;
  other.depth_ = 0;
  other.hash_functions_.clear();
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if (this == &other) {
    return *this;
  }

  width_ = other.width_;
  depth_ = other.depth_;
  counters_ = std::move(other.counters_);

  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  other.width_ = 0;
  other.depth_ = 0;
  other.hash_functions_.clear();
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for (uint32_t row = 0; row < depth_; row++) {
    const size_t col = hash_functions_[row](item);
    const size_t idx = static_cast<size_t>(row) * width_ + col;
    counters_[idx].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  const auto total_buckets = static_cast<size_t>(width_) * static_cast<size_t>(depth_);
  for (size_t i = 0; i < total_buckets; i++) {
    const uint32_t delta = other.counters_[i].load(std::memory_order_relaxed);
    counters_[i].fetch_add(delta, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  if (counters_ == nullptr || width_ == 0 || depth_ == 0) {
    return 0;
  }

  uint32_t result = std::numeric_limits<uint32_t>::max();
  for (uint32_t row = 0; row < depth_; row++) {
    const size_t col = hash_functions_[row](item);
    const size_t idx = static_cast<size_t>(row) * width_ + col;
    const uint32_t value = counters_[idx].load(std::memory_order_relaxed);
    result = std::min(result, value);
  }
  return result;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  if (counters_ == nullptr) {
    return;
  }

  const auto total_buckets = static_cast<size_t>(width_) * static_cast<size_t>(depth_);
  for (size_t i = 0; i < total_buckets; i++) {
    counters_[i].store(0, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  if (k == 0 || candidates.empty()) {
    return {};
  }

  std::vector<std::pair<KeyType, uint32_t>> ranked;
  ranked.reserve(candidates.size());

  for (const auto &candidate : candidates) {
    ranked.emplace_back(candidate, Count(candidate));
  }

  std::stable_sort(ranked.begin(), ranked.end(), [](const auto &a, const auto &b) { return a.second > b.second; });
  if (ranked.size() > k) {
    ranked.resize(k);
  }
  return ranked;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
