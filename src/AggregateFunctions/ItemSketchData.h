#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>

#include <memory>
#include <string>
#include <string_view>

#include <AggregateFunctions/SketchDataUtils.h>
#include <Core/Types.h>
#include <frequent_items_sketch.hpp>

namespace DB
{

/// The datasketches Frequent Items sketch corresponds to ItemsSketch in datasketches-java frequencies package.
using FrequentItemsSketch = datasketches::frequent_items_sketch<std::string, UInt64>;

static constexpr uint8_t DEFAULT_LG_MAX_MAP_SIZE = 10; // reasonable default for a small number of tracked items

class ItemSketchData final : private boost::noncopyable
{
private:
    std::unique_ptr<FrequentItemsSketch> sketch;
    uint8_t lg_max_map_size = DEFAULT_LG_MAX_MAP_SIZE;
    uint8_t lg_start_map_size = FrequentItemsSketch::LG_MIN_MAP_SIZE;

    FrequentItemsSketch * getOrCreate()
    {
        if (!sketch)
            sketch = std::make_unique<FrequentItemsSketch>(lg_max_map_size, lg_start_map_size);
        return sketch.get();
    }

public:
    ItemSketchData() = default;

    explicit ItemSketchData(uint8_t lg_max_map_size_)
        : lg_max_map_size(lg_max_map_size_)
    {}

    void insertOriginal(std::string_view item, UInt64 weight = 1)
    {
        if (weight == 0)
            return;
        /// datasketches expects an owned item type (std::string for this sketch specialization)
        getOrCreate()->update(std::string(item), weight);
    }

    void insertSerialized(std::string_view serialized_data, bool force_raw = true)
    {
        if (serialized_data.empty())
            return;

        std::string decoded_storage;
        auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, force_raw);
        if (data_ptr == nullptr || data_size == 0)
            return;

        try
        {
            auto other = FrequentItemsSketch::deserialize(data_ptr, data_size);
            if (!sketch)
                sketch = std::make_unique<FrequentItemsSketch>(std::move(other));
            else
                sketch->merge(other);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Skip invalid/corrupted sketches.
        }
    }

    String serializedData() const
    {
        if (!sketch)
            return "";
        auto bytes = sketch->serialize();
        return String(bytes.begin(), bytes.end());
    }

    void merge(const ItemSketchData & rhs)
    {
        if (!rhs.sketch)
            return;

        if (!sketch)
        {
            auto bytes = rhs.sketch->serialize();
            sketch = std::make_unique<FrequentItemsSketch>(FrequentItemsSketch::deserialize(bytes.data(), bytes.size()));
            return;
        }

        sketch->merge(*rhs.sketch);
    }

    void read(DB::ReadBuffer & in)
    {
        FrequentItemsSketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto other = FrequentItemsSketch::deserialize(bytes.data(), bytes.size());
            if (!sketch)
                sketch = std::make_unique<FrequentItemsSketch>(std::move(other));
            else
                sketch->merge(other);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if (sketch)
        {
            auto bytes = sketch->serialize();
            writeVectorBinary(bytes, out);
        }
        else
        {
            FrequentItemsSketch::vector_bytes bytes;
            writeVectorBinary(bytes, out);
        }
    }

    UInt64 estimate(std::string_view item) const
    {
        if (!sketch)
            return 0;
        return sketch->get_estimate(std::string(item));
    }
};

}

#endif

