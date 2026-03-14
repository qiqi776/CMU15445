// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock lock(latch_);

  if (curr_size_ == 0) {
    return std::nullopt;
  }

  auto evict_from = [&](std::list<frame_id_t> &live_list, ArcStatus from_status,
                        ArcStatus ghost_status) -> std::optional<frame_id_t> {
    for (auto it = live_list.rbegin(); it != live_list.rend(); ++it) {
      frame_id_t victim_frame_id = *it;

      auto alive_it = alive_map_.find(victim_frame_id);
      BUSTUB_ASSERT(alive_it != alive_map_.end(), "frame in list missing from alive_map_");

      auto entry = alive_it->second;
      BUSTUB_ASSERT(entry->arc_status_ == from_status, "frame status does not match live list");

      if (!entry->evictable_) {
        continue;
      }
      page_id_t victim_page_id = entry->page_id_;
      BUSTUB_ASSERT(ghost_map_.find(victim_page_id) == ghost_map_.end(), "page already exists in ghost list");

      auto pos = alive_pos_.find(victim_frame_id);
      BUSTUB_ASSERT(pos != alive_pos_.end(), "frame in alive_map_ missing from alive_pos_");

      live_list.erase(pos->second);
      alive_pos_.erase(pos);
      alive_map_.erase(alive_it);

      entry->frame_id_ = INVALID_FRAME_ID;
      entry->evictable_ = false;
      entry->arc_status_ = ghost_status;

      if (ghost_status == ArcStatus::MRU_GHOST) {
        mru_ghost_.push_front(victim_page_id);
        ghost_pos_[victim_page_id] = mru_ghost_.begin();
      } else {
        BUSTUB_ASSERT(ghost_status == ArcStatus::MFU_GHOST, "invalid ghost status");
        mfu_ghost_.push_front(victim_page_id);
        ghost_pos_[victim_page_id] = mfu_ghost_.begin();
      }
      ghost_map_[victim_page_id] = entry;
      curr_size_--;
      return victim_frame_id;
    }
    return std::nullopt;
  };
  if (mru_.size() >= mru_target_size_) {
    if (auto victim = evict_from(mru_, ArcStatus::MRU, ArcStatus::MRU_GHOST); victim.has_value()) {
      return victim;
    }
    return evict_from(mfu_, ArcStatus::MFU, ArcStatus::MFU_GHOST);
  }

  if (auto victim = evict_from(mfu_, ArcStatus::MFU, ArcStatus::MFU_GHOST); victim.has_value()) {
    return victim;
  }
  return evict_from(mru_, ArcStatus::MRU, ArcStatus::MRU_GHOST);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) <= replacer_size_, "invalid frame id");
  std::scoped_lock lock(latch_);

  auto it = alive_map_.find(frame_id);
  if (it != alive_map_.end()) {
    auto entry = it->second;
    BUSTUB_ASSERT(entry->page_id_ == page_id, "frame is already associated with a different page");

    auto pos = alive_pos_.find(frame_id);
    BUSTUB_ASSERT(pos != alive_pos_.end(), "missing alive position");

    if (entry->arc_status_ == ArcStatus::MRU) {
      mru_.erase(pos->second);
    } else if (entry->arc_status_ == ArcStatus::MFU) {
      mfu_.erase(pos->second);
    } else {
      BUSTUB_ASSERT(false, "alive entry must be in MRU or MFU");
    }

    mfu_.push_front(frame_id);
    alive_pos_[frame_id] = mfu_.begin();
    entry->arc_status_ = ArcStatus::MFU;
    return;
  }

  auto ghost_it = ghost_map_.find(page_id);
  if (ghost_it != ghost_map_.end()) {
    auto entry = ghost_it->second;
    auto ghost_pos_it = ghost_pos_.find(page_id);
    BUSTUB_ASSERT(ghost_pos_it != ghost_pos_.end(), "missing ghost position");

    if (entry->arc_status_ == ArcStatus::MRU_GHOST) {
      size_t delta = 1;
      if (mru_ghost_.size() < mfu_ghost_.size()) {
        delta = mfu_ghost_.size() / mru_ghost_.size();
      }
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta);
      mru_ghost_.erase(ghost_pos_it->second);
    } else if (entry->arc_status_ == ArcStatus::MFU_GHOST) {
      size_t delta = 1;
      if (mfu_ghost_.size() < mru_ghost_.size()) {
        delta = mru_ghost_.size() / mfu_ghost_.size();
      }
      if (delta > mru_target_size_) {
        mru_target_size_ = 0;
      } else {
        mru_target_size_ -= delta;
      }
      mfu_ghost_.erase(ghost_pos_it->second);
    } else {
      BUSTUB_ASSERT(false, "ghost entry must be in MRU_GHOST or MFU_GHOST");
    }

    ghost_pos_.erase(page_id);
    ghost_map_.erase(ghost_it);

    auto new_entry = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    mfu_.push_front(frame_id);
    alive_map_[frame_id] = new_entry;
    alive_pos_[frame_id] = mfu_.begin();
    return;
  }

  size_t mru_side_size = mru_.size() + mru_ghost_.size();
  size_t total = mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size();

  if (mru_side_size == replacer_size_) {
    BUSTUB_ASSERT(!mru_ghost_.empty(), "mru ghost should not be empty when mru side is full");
    page_id_t victim_page_id = mru_ghost_.back();
    mru_ghost_.pop_back();
    ghost_pos_.erase(victim_page_id);
    ghost_map_.erase(victim_page_id);
  } else if (mru_side_size < replacer_size_ && total == 2 * replacer_size_) {
    BUSTUB_ASSERT(!mfu_ghost_.empty(), "mfu ghost should not be empty when total size reaches 2c");
    page_id_t victim_page_id = mfu_ghost_.back();
    mfu_ghost_.pop_back();
    ghost_pos_.erase(victim_page_id);
    ghost_map_.erase(victim_page_id);
  }
  
  auto new_entry = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  mru_.push_front(frame_id);
  alive_map_[frame_id] = new_entry;
  alive_pos_[frame_id] = mru_.begin();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) <= replacer_size_, "invalid frame id");
  std::scoped_lock lock(latch_);

  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  if (it->second->evictable_ == set_evictable) {
    return;
  }

  it->second->evictable_ = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  auto entry = it->second;
  BUSTUB_ASSERT(entry->evictable_, "cannot remove a non-evictable frame");

  if (entry->arc_status_ == ArcStatus::MRU) {
    auto pos = alive_pos_.find(frame_id);
    BUSTUB_ASSERT(pos != alive_pos_.end(), "missing MRU position");
    mru_.erase(pos->second);
  } else if (entry->arc_status_ == ArcStatus::MFU) {
    auto pos = alive_pos_.find(frame_id);
    BUSTUB_ASSERT(pos != alive_pos_.end(), "missing MFU position");
    mfu_.erase(pos->second);
  } else {
    BUSTUB_ASSERT(false, "alive entry must be in MRU or MFU");
  }

  alive_pos_.erase(frame_id);
  alive_map_.erase(it);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub
