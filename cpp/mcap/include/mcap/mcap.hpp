#pragma once

#include "reader.hpp"

#ifndef MCAP_NO_PARALLEL
// Parallel (multithreaded) reader. Purely additive: provides the standalone
// mcap::ParallelReader (and MmapReader) without modifying McapReader. The only
// touch to an existing type is IReadable::supportsConcurrentRead() in reader.hpp.
// Define MCAP_NO_PARALLEL to omit it entirely (e.g. for platforms that can't link
// a threading library).
#  include "byte_semaphore.hpp"
#  include "mmap_reader.hpp"
#  include "parallel_budget.hpp"
#  include "parallel_reader.hpp"
#  include "thread_pool.hpp"
#endif

#include "writer.hpp"
